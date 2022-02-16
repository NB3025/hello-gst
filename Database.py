import re
import pymysql
import json
import os

HERE = os.path.abspath(os.path.dirname(__file__))

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        else:
            cls._instances[cls].__init__(*args,**kwargs)
        
        return cls._instances[cls]


class Database(metaclass=Singleton):

    def __init__(self):
        self._db_info_file = os.path.join(HERE,'gst_config/db_info.json')

        with open(self._db_info_file,'r') as f:
            self._db_obj = json.load(f)

        self._db = pymysql.connect(host=self._db_obj['host'],
                                    user=self._db_obj['user'],
                                    password=self._db_obj['password'],
                                    database=self._db_obj['database'])
        self._cursor = self._db.cursor(pymysql.cursors.DictCursor)

    def execute(self, query, args={}):
        self._cursor.execute(query,args)

    def executeOne(self, query, args={}):
        self._cursor.execute(query, args)
        row = self._cursor.fetchone()
        return row
 
    def executeAll(self, query, args={}):
        self._cursor.execute(query, args)
        row = self._cursor.fetchall()
        return row
 
    def commit(self):
        self._db.commit()
 