from turtle import pd
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import requests
import json
import time
import pymysql.cursors


class GST:
    def __init__(self):
        self._amount_url = 'https://api.solscan.io/account/tokens?address=CwwMfXPXfRT5H5JUatpBctASRGhKW2SqLWWGU3eX5Zgo&price=1'
        self._holders_url = 'https://api.solscan.io/token/holders?token=AFbX8oGjGpmVFywbVouvhQSRmiW2aR1mohfahi4Y2AdB&offset=0&size=20'
        
    # @property
    # def getURL(self):
    #     return self._url
    
    def getHolders(self):
        response = requests.get(self._holders_url)
        obj = response.text
        
        with open('db_info.json','r') as f:
            db_obj = json.load(f)

        connection = pymysql.connect(host=db_obj['host'],
                                    user=db_obj['user'],
                                    password=db_obj['password'],
                                    database=db_obj['database'],
                                    cursorclass=pymysql.cursors.DictCursor)
        
        with connection:
            with connection.cursor() as cursor:
                sql = f"Insert INTO t_gst_holders (data) VALUES (%s);"
                cursor.execute(sql,(obj))
    
            connection.commit()
            
    def getAmount(self):
        response = requests.get(self._amount_url)
        obj = json.loads(response.text)

        target_sysmbol = ['USDC', 'GST']

        amount_dict = {}
        for obj_data in obj['data']:
            tokenSymbol = obj_data['tokenSymbol']
            if tokenSymbol in target_sysmbol:
                amount = round(obj_data['tokenAmount']['uiAmount'],0)
                amount_dict[tokenSymbol] = amount
        
        with open('db_info.json','r') as f:
            db_obj = json.load(f)

        connection = pymysql.connect(host=db_obj['host'],
                                    user=db_obj['user'],
                                    password=db_obj['password'],
                                    database=db_obj['database'],
                                    cursorclass=pymysql.cursors.DictCursor)

        with connection:
            with connection.cursor() as cursor:
                sql = f"Insert INTO t_token_amount(usdc_amount, gst_amount) VALUES({amount_dict['USDC']},{amount_dict['GST']})"
                cursor.execute(sql)
    
            connection.commit()
    
            # with connection.cursor() as cursor:
            #     sql = "SELECT * FROM t_token_amount"
            #     cursor.execute(sql)
            #     result = cursor.fetchone()
            #     print (result)                    
        
    def savejson(self):
        response = requests.get(gst.getURL)        
        l_time = time.localtime(time.time())
        fname = f"{time.strftime('%Y%m%d%I%M%S',l_time)}.json"

        f = open(fname, 'w')
        f.write(response.text)
        f.close()
        
        print (fname)

gst = GST()
gst.getAmount()
gst.getHolders()

