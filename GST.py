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
        
        self.notify_amount()
    
    def notify_amount(self):
        sql = "select * from t_token_amount order by created_at desc limit 12" # 1시간
        result = self._db_manager.executeAll(sql)

        cur_gst_amount = result[0]['gst_amount']
        pre_gst_amount = result[-1]['gst_amount']
        
        if (cur_gst_amount - pre_gst_amount) < 50000:
            return

        l_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with open(TELEGRAM_CON,'r') as f:
            telegram_obj = json.load(f)
        chat_list = telegram_obj['chat_list']
        key = telegram_obj['KEY']

        notify_msg = f"{l_time} GST 갯수 5만개 이상 변동 알림 \n 현재 GST수량 : {cur_gst_amount} \n 이전 GST수량 : {pre_gst_amount}"
        for chat in chat_list:
            tel_url = f"https://api.telegram.org/bot{key}/sendmessage?chat_id={chat}&text={notify_msg}"
            res = requests.get(tel_url)

        print (f'[notify_price] {notify_msg=}')
        
    
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
        price_list = self.get_price()
        # TODO 처음  DB를 쌓는경우 price_list에 데이터가 없음. 처리해주야함
        
        min_price = price_list['min']
        max_price = price_list['max']
        avg_price = price_list['avg']

        print (f'[notify_price] {max_price=} / {min_price=} /{cur_price=} / {avg_price=}')

        if cur_price > avg_price*1.005:
            status = "상승 중"
        elif cur_price < avg_price*0.995:
            status = "하락 중"
        else:
            return 

        l_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with open(TELEGRAM_CON,'r') as f:
            telegram_obj = json.load(f)
        chat_list = telegram_obj['chat_list']
        key = telegram_obj['KEY']

        print (f'[notify_price] {max_price=} / {min_price=} /{cur_price=} / {avg_price=}')

        notify_msg = f"{l_time} 가격 알림 ***{status}***\n 현재 가격 : {round(cur_price,3)} \n 이전 1시간 최고 가격 : {round(max_price,3)} \n 이전 1시간 최저 가격 : {round(min_price,3)}"
        for chat in chat_list:
            tel_url = f"https://api.telegram.org/bot{key}/sendmessage?chat_id={chat}&text={notify_msg}"
            res = requests.get(tel_url)

        print (f'[notify_price] {notify_msg=}')

    def get_price(self):
        sql = "select price from t_gst_price order by created_at desc limit 12" # 1시간
        result = self._db_manager.executeAll(sql)
        result = result[:len(result)-1]

        price_list = []
        for r in result:
            price_list.append(r['price'])
        
        price_dict = {
            'min': min(price_list),
            'max': max(price_list),
            'avg':(sum(price_list)/len(price_list))
            }
        
        return price_dict


gst = GST()
gst.set_amount()
gst.set_holders()
gst.set_price()