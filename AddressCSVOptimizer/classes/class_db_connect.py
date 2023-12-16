from utils.performance import performance
import mysql.connector


class DBConnect:
    _host = "http://localhost/"
    _database = "database_name"
    _dbuser = "username"
    _dbpassword = "password"
    _port = 80

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
    def connect(*args, **kwargs):
        host = args[0]
        database = args[1]
        user = args[2]
        password = args[3]
        port = args[4]

        return mysql.connector.connect(host=host, database=database, user=user, password=password, port=port)
