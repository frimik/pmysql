README
This is a fork ok pmysql from mysqlatfacebook, created by Domas Mituzas.
This fork adds extra functionality for database servers which run multiple instances of MySQL/MariaDB defined by their port number.


ORIGINAL README
This is parallel mysql client, usage:

pmysql "QUERY" < server-list

Usernames and passwords are picked from [client] and 
[pmysql] sections at my.cnf (and ~/.my.cnf)
