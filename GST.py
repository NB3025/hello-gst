from turtle import pd
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import requests
import json
import time


class GST:
    def __init__(self):
        self._url = 'https://api.solscan.io/account/tokens?address=CwwMfXPXfRT5H5JUatpBctASRGhKW2SqLWWGU3eX5Zgo&price=1'

    @property
    def getURL(self):
        return self._url
    
    def getAmount(self):
        response = requests.get(gst.getURL)
        obj = json.loads(response.text)

        target_sysmbol = ['USDC', 'GST']

        amount_dict = {}
        for obj_data in obj['data']:
            tokenSymbol = obj_data['tokenSymbol']
            if tokenSymbol in target_sysmbol:
                amount = round(obj_data['tokenAmount']['uiAmount'],0)
                amount_dict[tokenSymbol] = amount
        
        return amount_dict
    
    def run_sql(self, amount_dict):
    
        conn = sqlite3.connect('token.db3')
        cur = conn.cursor()

        _sql = f"Insert INTO amount(USDC_amount, GST_amount) VALUES({amount_dict['USDC']},{amount_dict['GST']})"
        cur.execute(_sql)

        conn.commit()
                    
        
    def savejson(self):
        response = requests.get(gst.getURL)        
        l_time = time.localtime(time.time())
        fname = f"{time.strftime('%Y%m%d%I%M%S',l_time)}.json"

        f = open(fname, 'w')
        f.write(response.text)
        f.close()
        
        print (fname)

while 1:

    gst = GST()
    amount_dict = gst.getAmount()
    gst.run_sql(amount_dict)
    
    time.sleep(300)


