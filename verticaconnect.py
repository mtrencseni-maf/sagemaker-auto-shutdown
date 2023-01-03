import vertica_python
import pandas as pd
import configparser
import os

class Vertica:
    def __init__(self, config=None, path='config.ini'):
        if not config:
            config = self.get_config('config.ini')
        self.host = str(config["vertica"]["host"]).strip()
        self.port = int(config["vertica"]["port"])
        self.user = str(config["vertica"]["user"]).strip()
        self.pswd = str(config["vertica"]["password"]).strip()
        self.data_base = str(config["vertica"]["database"]).strip()

    @staticmethod
    def get_config(config_file_name):
        parent_dir_path = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(parent_dir_path,config_file_name)
        config = configparser.ConfigParser()
        config.read(config_path)
        return config

    def _getconnection(self):
        conninfo = {'host': self.host,
                    'port': self.port,
                    'user': self.user,
                    'password': self.pswd,
                    'database': self.data_base,
                    'connection_timeout': 1000}
        return conninfo

    def _get_con(self):
        conn = vertica_python.connect(**self._getconnection())
        return conn

    def _execute_query(self, query):
        cursor = vertica_python.connect(**self._getconnection()).cursor()
        cursor.execute(query)
        return cursor

    def query(self, sql):
        import time
        start_time = time.time()
        cursor = self._execute_query(sql)
        df = pd.DataFrame(
            cursor.fetchall(), columns=[
                x[0] for x in cursor.description])
        cursor.close()
        return df