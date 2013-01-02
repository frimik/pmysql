CXXFLAGS+=-Wall -Werror -g -O3 `mysql_config --cflags` `pkg-config glib-2.0 gthread-2.0 --cflags`
LDFLAGS+=`mysql_config --libs_r` `pkg-config glib-2.0 gthread-2.0 --libs`

all: pmysql

install: pmysql
	install -D pmysql ${DESTDIR}/usr/bin/pmysql

clean:
	rm -f pmysql
