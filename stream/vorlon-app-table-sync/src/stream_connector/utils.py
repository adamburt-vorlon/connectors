import psycopg2
from psycopg2._psycopg import connection as PGConnection
from psycopg2.errors import UniqueViolation
from pymongo import MongoClient
import re


def postgres_connect(connection_string: str, db_name: str = None) -> PGConnection:
    re_host = re.compile("host=(\\S+)\\s|$")
    re_port = re.compile("port=(\\S+)\\s|$")
    re_user = re.compile("user=(\\S+)\\s|$")
    re_password = re.compile("password=(\\S+)\\s|$")
    re_db = re.compile("dbname=(\\S+)")
    if "host=" in connection_string:
        host = re_host.findall(connection_string)[0]
    else:
        host=None
    if "port=" in connection_string:
        port = int(re_port.findall(connection_string)[0])
    else:
        port=None
    if "user=" in connection_string:
        user = re_user.findall(connection_string)[0]
    else:
        user=None
    if "password=" in connection_string:
        password = re_password.findall(connection_string)[0]
    else:
        password=None
    if db_name:
        db = db_name
    elif "dbname=" in connection_string:
        db = re_db.findall(connection_string)[0]
    else:
        db = None
    try:
        conn: PGConnection = psycopg2.connect(
            host=host,
            port=port,
            database=db,
            user=user,
            password=password
        )
        return conn
    except Exception as err:
        return None


def mongodb_connect(connection_string: str) -> MongoClient:
    mongo_client: MongoClient = MongoClient(connection_string)
    return mongo_client
