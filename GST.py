import requests
import json
import time
import os
import pandas as pd
from datetime import datetime
from Database import Database

HERE = os.path.abspath(os.path.dirname(__file__))
TELEGRAM_CON = os.path.join(HERE, 'gst_config/telegram.json')
        # self._db_info_file = os.path.join(HERE,'db_info.json')

class GST:
    def __init__(self):
        self._amount_url = 'https://api.solscan.io/account/tokens?address=CwwMfXPXfRT5H5JUatpBctASRGhKW2SqLWWGU3eX5Zgo&price=1'
        self._holders_url = 'https://api.solscan.io/token/holders?token=AFbX8oGjGpmVFywbVouvhQSRmiW2aR1mohfahi4Y2AdB&offset=0&size=20'
        self._price_url = 'https://api.solscan.io/amm/market?address=AFbX8oGjGpmVFywbVouvhQSRmiW2aR1mohfahi4Y2AdB&sort_by=liquidity&sort_type=desc'
        self._db_manager = Database()
        
    def set_holders(self):
        response = requests.get(self._holders_url)
        obj = response.text

        sql = f"Insert INTO t_gst_holders (data) VALUES (%s);"
        self._db_manager.execute(sql,obj)
        self._db_manager.commit()

    def set_amount(self):
        response = requests.get(self._amount_url)
        obj = json.loads(response.text)

        target_sysmbol = ['USDC', 'GST']

        amount_dict = {}
        for obj_data in obj['data']:
            tokenSymbol = obj_data['tokenSymbol']
            if tokenSymbol in target_sysmbol:
                amount = round(obj_data['tokenAmount']['uiAmount'],0)
                amount_dict[tokenSymbol] = amount
        
        sql = f"Insert INTO t_token_amount(usdc_amount, gst_amount) VALUES({amount_dict['USDC']},{amount_dict['GST']})"
        self._db_manager.execute(sql)
        self._db_manager.commit()
    
    def get_amount(self):
        df = pd.DataFrame()
        with self._db_manager:
            with self._db_manager.cursor() as cursor:
                sql = "SELECT * FROM t_token_amount"
                cursor.execute(sql)
                result = cursor.fetchall()
                for r in result:
                    df_ = pd.DataFrame([r])
                    df = pd.concat([df,df_])
                
        df.to_csv('getamount.csv')
    
    def save_json(self):
        response = requests.get(gst.getURL)        
        l_time = time.localtime(time.time())
        fname = f"{time.strftime('%Y%m%d%I%M%S',l_time)}.json"

        f = open(fname, 'w')
        f.write(response.text)
        f.close()
        
        print (fname)

    def set_price(self):
        response = requests.get(self._price_url)
        obj = json.loads(response.text)

        max_volume = -1
        exchange = ''
        price = 0
        for o in obj['data']:
            _vol = o['volume24h']
            _pri = o['price']
            _exc = o['source']
            if max_volume < _vol:
                max_volume = _vol
                exchange = _exc
                price = _pri
      
        sql = f"Insert INTO t_gst_price(price, exchange) VALUES({price},'{exchange}')"
        self._db_manager.execute(sql)
        self._db_manager.commit()
        
        self.notify_price(price)

    def notify_price(self,cur_price):
        pre_price = self.get_price()
        max_price = max(pre_price)

        diff = (cur_price/max_price)-1

        l_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with open(TELEGRAM_CON,'r') as f:
            telegram_obj = json.load(f)
        chat_list = telegram_obj['chat_list']
        key = telegram_obj['KEY']

        print (f'[notify_price] {pre_price=} / {max_price=} / {cur_price=}')

        if abs(diff) > 0.03:
            notify_msg = f"{l_time} 가격 알림 \n 현재 가격 : {round(cur_price,3)} \n 이전 1시간 최고 가격 : {round(max_price,3)} "
            for chat in chat_list:
                tel_url = f"https://api.telegram.org/bot{key}/sendmessage?chat_id={chat}&text={notify_msg}"
                res = requests.get(tel_url)
            print (f'[notify_price] {notify_msg=}')

    def get_price(self):
        price_arr = []

        sql = "SELECT * FROM t_gst_price limit 12"
        result = self._db_manager.executeAll(sql)
        for r in result:
            price_arr.append(r['price'])
        
        return price_arr


gst = GST()
gst.set_amount()
gst.set_holders()
gst.set_price()