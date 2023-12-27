from dotenv import dotenv_values
from utils.performance import performance
import mysql.connector

config = dotenv_values(".env")

class DBConnect:
    _host = config['HOST']
    _database = config['DATABASE_NAME']
    _dbuser = config['DATABASE_USER']
    _dbpassword = config['DATABASE_PASSWORD']
    _port = config['PORT']

    def __init__(self):
        pass

    def set_host(self, host):
        self._host = host

    def set_database(self, database):
        self._database = database

    def set_dbuser(self, db_user):
        self._dbuser = db_user

    def set_dbpassword(self, db_password):
        self._db_password = db_password

    def set_port(self, port):
        self._port = port

    def get_host(self):
        return self._host

    def get_database(self):
        return self._database

    def get_dbuser(self):
        return self._dbuser

    def get_dbpassword(self):
        return self._dbpassword

    def get_port(self):
        return self._port

    @performance
    @staticmethod
    def connect(*args):
        host = args[0]
        database = args[1]
        user = args[2]
        password = args[3]
        port = args[4]

        try:
            return mysql.connector.connect(host=host, database=database, user=user, password=password, port=port)
        except KeyError:
            print(*args)
