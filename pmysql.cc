
#include <mysql.h>
#include <stdio.h>
#include <string.h>
#include <glib.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>

/* Allow providing special meta database to skip at --all */
#ifndef METADB
#define METADB ""
#endif

char *username = NULL;
char *password = NULL;
int port = 0;
char *socket_path = NULL;
char *db = NULL;
char **databases = NULL;

gboolean all_databases = false;

char *query = NULL;
char *queryfile = NULL;

char *serversfile = NULL;

int num_threads = 200;

#define CONNECT_TIMEOUT 2
int connect_timeout = 0;

int read_timeout=0;

gboolean should_escape = 0;
gboolean should_compress = 0;

static GOptionEntry entries[] = {
    {"query", 'Q', 0, G_OPTION_ARG_STRING, &query, "Queries to run", NULL},
    {"query-file", 'F', 0, G_OPTION_ARG_STRING, &queryfile, "File to read queries from", NULL},
    {"servers-file", 'X', 0, G_OPTION_ARG_STRING, &serversfile, "File to read servers from (stdin otherwise)", NULL},
    {"user", 'u', 0, G_OPTION_ARG_STRING, &username, "Username with privileges to run the dump", NULL},
    {"password", 'p', 0, G_OPTION_ARG_STRING, &password, "User password", NULL},
    {"port", 'P', 0, G_OPTION_ARG_INT, &port, "TCP/IP port to connect to", NULL},
    {"socket", 'S', 0, G_OPTION_ARG_STRING, &socket_path, "UNIX domain socket file to use for connection", NULL},
    {"database", 'B', 0, G_OPTION_ARG_STRING, &db, "Databases (comma-separated) to run query against", NULL},
    {"all", 'A', 0, G_OPTION_ARG_NONE, &all_databases, "Run on all databases except i_s, mysql and test", NULL},
    {"threads", 't', 0, G_OPTION_ARG_INT, &num_threads, "Number of parallel threads", NULL},
    {"escape", 'e', 0, G_OPTION_ARG_NONE, &should_escape, "Should tabs, newlines and zero bytes be escaped", NULL},
    {"compress", 'c', 0, G_OPTION_ARG_NONE, &should_compress, "Compress server-client communication", NULL},
    {"connect-timeout", 'T', 0, G_OPTION_ARG_INT, &connect_timeout, "Connect timeout in seconds (default: 2)", NULL},
    {"read-timeout", 'R', 0, G_OPTION_ARG_INT, &read_timeout, "Read timeout in seconds", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}
};

static GMutex *write_mutex = NULL;


void write_g_string(int fd, GString * data)
{
    ssize_t written = 0, r = 0;

    g_mutex_lock(write_mutex);
    while (written < (ssize_t) data->len) {
        r = write(fd, data->str + written, data->len);
        if (r < 0) {
            g_critical("Couldn't write output data to a file: %s", strerror(errno));
            exit(EXIT_FAILURE);
        }
        written += r;
    }
    g_mutex_unlock(write_mutex);
}

/* Escape few whitespace chars, needs preallocated output buffer and source length provided */
gulong line_escape(char *from, gulong length, char *to)
{

    char *p = from;
    char *t = to;

    while (length--) {
        switch (*p) {
        case '\n':
            *t++ = '\\';
            *t++ = 'n';
            break;
        case '\t':
            *t++ = '\\';
            *t++ = 't';
            break;
        case 0:
            *t++ = '\\';
            *t++ = '0';
            break;
        default:
            *t++ = *p;
        }
        p++;
    }
    *t++ = 0;
    return t - to;
}


void run_query(char *query, MYSQL *mysql, char *server_name, char *db_name) {
    int status;
    int num_fields;

    MYSQL_RES *res = NULL;
    MYSQL_ROW row = NULL;

    GString *rowtext = NULL;
    GString *escaped = NULL;

    if (db_name) {
        if (mysql_select_db(mysql, db_name)) {
            g_warning("Could not select db %s on %s: %s", db_name, server_name, mysql_error(mysql));
            return;
        }
    }

    if ((status = mysql_query(mysql, query))) {
        g_warning("Could not execute query on %s: %s", server_name, mysql_error(mysql));
    }

    rowtext = g_string_sized_new(1024);
    escaped = g_string_sized_new(2048);
    do {
        res = mysql_use_result(mysql);
        if (res) {
            num_fields = mysql_num_fields(res);
    
            while ((row = mysql_fetch_row(res))) {
                unsigned long *lengths = mysql_fetch_lengths(res);
                g_string_printf(rowtext, "%s%s%s\t", server_name, db_name?"\t":"", db_name?db_name:"");
    
                for (int i = 0; i < num_fields; i++) {
                    if (!should_escape || !row[i]) {
                        g_string_append(rowtext, row[i] ? row[i] : "\\N");
                        g_string_append(rowtext, (num_fields - i == 1) ? "\n" : "\t");
                    } else {
                        g_string_set_size(escaped, lengths[i] * 2 + 1);
                        line_escape(row[i], lengths[i], escaped->str);
                        g_string_append(rowtext, escaped->str);
                        g_string_append(rowtext, (num_fields - i == 1) ? "\n" : "\t");
                    }
                }
                write_g_string(STDOUT_FILENO, rowtext);
            }

            if (mysql_errno(mysql))
                g_critical("Could not retrieve result set fully from %s: %s\n", server_name, mysql_error(mysql));

            mysql_free_result(res);
        } else {                /* no result set or error */

            if (mysql_field_count(mysql) != 0) {
                g_critical("Could not retrieve result set from %s: %s\n", server_name, mysql_error(mysql));
                break;
            }
        }
        if ((status = mysql_next_result(mysql)) > 0)
            g_critical("Could not execute statement on %s: %s", server_name, mysql_error(mysql));
    } while (status == 0);

    if (rowtext)
        g_string_free(rowtext, true);

    if (escaped)
        g_string_free(escaped, true);
}

static void worker_thread(gpointer data, gpointer user_data)
{
    int status;
    
    /* "server", "host" and "query" */
    char *s = (char *) data;
    char *h = strdup(s);
    char *q = (char *) user_data;

    mysql_thread_init();
    MYSQL *mysql = mysql_init(NULL);
    MYSQL_RES *res = NULL;
    MYSQL_ROW row = NULL;

    gulong client_flags = CLIENT_MULTI_STATEMENTS;

    if (should_compress)
        client_flags |= CLIENT_COMPRESS;

    /* Do we have a special port? */
    int spec_port = 0;
    char *p = strstr(h, ":");
    if (p) {
        *p++ = 0;
        spec_port = atoi(p);
    }
    
    mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, (const char *)&connect_timeout);

    if (read_timeout)
        mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, (const char *)&connect_timeout);

    if (!mysql_real_connect(mysql, h, username, password, db, \
                            spec_port ? spec_port : port, socket_path, client_flags)) {
        g_warning("Could not connect to %s: %s", s, mysql_error(mysql));
        goto cleanup;
    }

    /* We run query on all databases except mysql,test,information_schema if --all is specified
       If --database has been specified, it is merged with full db list without checking for dupes
    */
    if (all_databases) {
        if ((status = mysql_query(mysql,"SHOW DATABASES"))) {
            g_warning("Could not get list of databases");
            goto cleanup;
        }
        
        if (!(res = mysql_store_result(mysql))) {
            g_warning("Could not get list of databases");
            goto cleanup;
        }
        
        while((row = mysql_fetch_row(res))) {
            if (strcmp(row[0], "mysql") && strcmp(row[0], "test") && strcmp(row[0], "information_schema")
                && strcmp(row[0], METADB))
                run_query(q, mysql, s, row[0]);
        }
        mysql_free_result(res);
        
		/* Merge --database if specified (both for multiple and single case) */
        if (databases) {
            for (int i=0; databases[i]; i++) {
                run_query(q, mysql, s, databases[i]);
            }
        } else if (db) {
            run_query(q, mysql, s, db);
        }
    } else {
        /* Run on all specified databases */
        if (databases) {
            for (int i=0; databases[i]; i++) {
                run_query(q, mysql, s, databases[i]);
            }
        } else {
            run_query(q, mysql, s, NULL);
        }
    }
    
  cleanup:
    mysql_close(mysql);
    mysql_thread_end();
    free(s);
    free(h);

}

int main(int argc, char **argv)
{
    char server[256];
    GError *error = NULL;

    FILE *serversfd;

    /* Command line option parsing */
    GOptionContext *context = g_option_context_new("[query]");
    g_option_context_set_summary(context, "Parallel multiple-server MySQL querying tool");
    g_option_context_add_main_entries(context, entries, NULL);
    if (!g_option_context_parse(context, &argc, &argv, &error)) {
        g_print("option parsing failed: %s, try --help\n", error->message);
        exit(EXIT_FAILURE);
    }
    g_option_context_free(context);

    /* Option postprocessing */
    if (queryfile) {
        if (query || argc > 1) {
            g_critical("Both query and query-file provided, they are mutually exclusive");
            exit(EXIT_FAILURE);
        }

        if (!g_file_get_contents(queryfile, &query, NULL, &error)) {
            g_critical("Could not read query file (%s): %s", queryfile, error->message);
            exit(EXIT_FAILURE);
        }
    }

    if ((query and argc > 1) or argc > 2) {
        g_critical("Multiple query arguments provided, use ; separated list for multiple queries");
        exit(EXIT_FAILURE);
    }

    if (!query and ! queryfile) {
        if (argc < 2) {
            g_critical("Need query!");
            exit(EXIT_FAILURE);
        }
        query = argv[1];
    }

    mysql_library_init(0, NULL, NULL);
    g_thread_init(NULL);
    MYSQL *mysql = mysql_init(NULL);
    mysql_thread_init();

    mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "pmysql");
    // This is fake connect to remember various options from my.cnf for other threads
    // Do note, this connect may succeed :-) 
    mysql_real_connect(mysql, NULL, NULL, NULL, NULL, 0, NULL, CLIENT_REMEMBER_OPTIONS);

    if (!username)
        username = mysql->options.user;
    if (!password)
        password = mysql->options.password;
    if (!port)
        port = mysql->options.port;

    /* Support for a list of databases */
    if (db && strstr(db, ",")) {
        databases = g_strsplit(db, ",", 0);
        *strstr(db,",") = 0;
    }

    if (!socket_path)
        socket_path = mysql->options.unix_socket;
    if (!connect_timeout) {
        if (mysql->options.connect_timeout)
            connect_timeout = mysql->options.connect_timeout; 
        else
            connect_timeout = CONNECT_TIMEOUT;
    }

    g_assert(write_mutex == NULL);
    write_mutex = g_mutex_new();


    GThreadPool *tp = g_thread_pool_new(worker_thread, query, num_threads, true, NULL);

    if (serversfile && strcmp(serversfile, "-")) {
        serversfd = fopen(serversfile, "r");
        if (!serversfd) {
            g_critical("Could not open servers list (%s): %s", serversfile, strerror(errno));
            exit(EXIT_FAILURE);
        }
    } else {
        serversfd = stdin;
    }
    while (fgets(server, 255, serversfd)) {
        char *nl = strstr(server, "\n");
        if (nl)
            nl[0] = 0;
        g_thread_pool_push(tp, (gpointer) strdup(server), NULL);
    }
    g_thread_pool_free(tp, 0, 1);
    exit(EXIT_SUCCESS);
}


/* vim: tabstop=8 
 * */
