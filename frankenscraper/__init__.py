import MySQLdb
import settings
import sys

TESTING = sys.argv[0].endswith('nosetests')

kwargs = {}
if settings.mysql_pw:
    kwargs['passwd'] = settings.mysql_pw
if settings.mysql_user:
    kwargs['user'] = settings.mysql_user
if settings.mysql_host:
    kwargs['host'] = settings.mysql_host
if settings.mysql_db:
    kwargs['db'] = settings.mysql_db
if settings.mysql_port:
    kwargs['port'] = settings.mysql_port

db = MySQLdb.connect(**kwargs)
