from websocket import create_connection
import json
import random
import string
import re
import pandas as pd
import csv
import sys

import ormsgpack, decimal

from datetime import datetime
from time import localtime


import datetime

if len(sys.argv) != 2:
        print("Usage: python script.py <days_delta>")
        sys.exit(1)

day_delta = int(sys.argv[1])


####SUPABASE INTERACT##

import os
from supabase import create_client, Client

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

## TEST creating user to auth table ####
# Create a random user login email and password.
# random_email: str = "test555@supamail.com"
# random_password: str = "fqj13bnf2hiu23h"
# user = supabase.auth.sign_up({ "email": random_email, "password": random_password, "username":"fireant" })

def default(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError

def filter_raw_message(text):
    try:
        found = re.search('"m":"(.+?)",', text).group(1)
        found2 = re.search('"p":(.+?"}"])}', text).group(1)
        print(found)
        print(found2)
        return found, found2
    except AttributeError:
        print("error")
    

def generateSession():
    stringLength=12
    letters = string.ascii_lowercase
    random_string= ''.join(random.choice(letters) for i in range(stringLength))
    return "qs_" +random_string

def generateChartSession():
    stringLength=12
    letters = string.ascii_lowercase
    random_string= ''.join(random.choice(letters) for i in range(stringLength))
    return "cs_" +random_string

def prependHeader(st):
    return "~m~" + str(len(st)) + "~m~" + st

def constructMessage(func, paramList):
    #json_mylist = json.dumps(mylist, separators=(',', ':'))
    return json.dumps({
        "m":func,
        "p":paramList
        }, separators=(',', ':'))

def createMessage(func, paramList):
    return prependHeader(constructMessage(func, paramList))

def sendRawMessage(ws, message):
    ws.send(prependHeader(message))

def sendMessage(ws, func, args):
    ws.send(createMessage(func, args))

def generate_csv(a):
    out= re.search('"s":\[(.+?)\}\]', a).group(1)
    x=out.split(',{\"')
    
    with open('data_file.csv', mode='w', newline='') as data_file:
        employee_writer = csv.writer(data_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    
        employee_writer.writerow(['index', 'date', 'open', 'high', 'low', 'close', 'volume'])
        
        for xi in x:
            xi= re.split('\[|:|,|\]', xi)
            print(xi)
            ind= int(xi[1])
            ts= datetime.fromtimestamp(float(xi[4])).strftime("%Y/%m/%d, %H:%M:%S")
            employee_writer.writerow([ind, ts, float(xi[5]), float(xi[6]), float(xi[7]), float(xi[8]), 'TODO'])
# add txt output file            
def create_output_file():
    now = localtime()
    fname = f"{now[0]}-{now[1]}-{now[2]}.txt"
    return fname            


# Initialize the headers needed for the websocket connection
headers = json.dumps({
    'Connection': 'upgrade',
    'Host': 'tradestation.fireant.vn',
    'Origin':'https://fireant.vn',
    'Cache-Control': 'no-cache',    
    'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits',
    'Sec-WebSocket-Key': 'FZxL0JOV3fmKy/RLg79/iQ==',
    'Sec-WebSocket-Version': '13',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Pragma': 'no-cache',
    'Upgrade': 'websocket'
})

    
# Then create a connection to the tunnel
ws = create_connection(
#    'wss://data.tradingview.com/socket.io/websocket',headers=headers)
'wss://tradestation.fireant.vn/quote?access_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg', headers=headers)

session= generateSession()
# print("session generated {}".format(session))

chart_session= generateChartSession()
# print("chart_session generated {}".format(chart_session))

def encode_json(obj):
    return json.dumps(obj) + chr(0x1E)

def ws_on_message(ws, message: str):
     print(message)

def format_invokeID(session, _target, prev_dt, to_dt):
    return "SESS_{session}_invoked_{target}_args_{prev}_to_{to}".format(
        session=session, 
        target=_target,
        prev=prev_dt,
        to=to_dt
    )

def format_dt_obj(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S") #fireANT doesnt accept 'Z' as ending

DAYS_DELTA = day_delta  ## prev day(s) delta for calculation prev day(s)


current_datetime = datetime.datetime.now()
prev_datetime_iso = format_dt_obj(current_datetime - datetime.timedelta(days=DAYS_DELTA))
current_datetime_iso = format_dt_obj(current_datetime)

# def toFireAntDTObj(dt):
#     return dt.split('T').any(t => t.slice_str_at(11))

print("Current datetime (ISO format):", current_datetime_iso)
print("Prev datetime (ISO format):", prev_datetime_iso)


def ws_on_open(ws):
    ws.send(encode_json({
        "protocol": "json",
        "version": 1
    }))

    # _target = "GetSymbols"

    # x = encode_json({
    #     "type": 1,
    #     "invocationId":format_invokeID(session, _target),
    #     "target": "{target}".format(target = _target),
    #     "arguments":[]
    # }) 

    _target = "GetBars"

    x = encode_json({
        "type": 1,
        "invocationId":format_invokeID(session, _target, prev_datetime_iso, current_datetime_iso),
        "target": "{target}".format(target = _target),
        "arguments":[
            "VN30",
            "1",
             prev_datetime_iso, 
             current_datetime_iso
        ]
    }) 
    
    ws.send(x)    

    ## "GetTradingStatisticsLastUpdated"
    # _target = "GetTradingStatisticsLastUpdated"

    # x = encode_json({
    #     "type": 1,
    #     "invocationId":format_invokeID(session, _target),
    #     "target": "{target}".format(target = _target),
    #     "arguments":[]
    # }) 
    
    # ws.send(x)    

ws_on_open(ws)

# Then send a message through the tunnel 
# sendMessage(ws, "set_auth_token", ["unauthorized_user_token"])
# sendMessage(ws, "set_access_token", ["eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg"])
# sendMessage(ws, "chart_create_session", [chart_session, ""])
# sendMessage(ws, "quote_create_session", [session])
# sendMessage(ws,"quote_set_fields", [session,"ch","chp","current_session","description","local_description","language","exchange","fractional","is_tradable","lp","lp_time","minmov","minmove2","original_name","pricescale","pro_name","short_name","type","update_mode","volume","currency_code","rchp","rtc"])
# sendMessage(ws, "quote_add_symbols",[session, "HOSE:VN30", {"flags":['force_permission']}])
# sendMessage(ws, "quote_fast_symbols", [session,"HOSE:VN30"])

#st='~m~140~m~{"m":"resolve_symbol","p":}'
#p1, p2 = filter_raw_message(st)

# sendMessage(ws, "resolve_symbol", [chart_session,"symbol_1","={\"symbol\":\"HOSE:VN30\",\"adjustment\":\"splits\",\"session\":\"extended\"}"])
# sendMessage(ws, "create_series", [chart_session, "s1", "s1", "symbol_1", "1", 10])

#sendMessage(ws, "create_study", [chart_session,"st4","st1","s1","ESD@tv-scripting-101!",{"text":"BNEhyMp2zcJFvntl+CdKjA==_DkJH8pNTUOoUT2BnMT6NHSuLIuKni9D9SDMm1UOm/vLtzAhPVypsvWlzDDenSfeyoFHLhX7G61HDlNHwqt/czTEwncKBDNi1b3fj26V54CkMKtrI21tXW7OQD/OSYxxd6SzPtFwiCVAoPbF2Y1lBIg/YE9nGDkr6jeDdPwF0d2bC+yN8lhBm03WYMOyrr6wFST+P/38BoSeZvMXI1Xfw84rnntV9+MDVxV8L19OE/0K/NBRvYpxgWMGCqH79/sHMrCsF6uOpIIgF8bEVQFGBKDSxbNa0nc+npqK5vPdHwvQuy5XuMnGIqsjR4sIMml2lJGi/XqzfU/L9Wj9xfuNNB2ty5PhxgzWiJU1Z1JTzsDsth2PyP29q8a91MQrmpZ9GwHnJdLjbzUv3vbOm9R4/u9K2lwhcBrqrLsj/VfVWMSBP","pineId":"TV_SPLITS","pineVersion":"8.0"}])


def slice_str_at(str,index):
    return str[:index]

# Printing all the result
a=""
outfilename = create_output_file()
while True:
    try:
        result = ws.recv()   
        if "SESS_" in result:
            # print(result)  
            result = slice_str_at(result, -1)
            # print(result)   
            json_obj = json.loads(result)
            # print(json_obj["result"])
            # supabase.table("fireant_symbol_bars").insert({
            #     "symbol": "VN30",
            #     "from_datetime":"2023-06-15T09:00:00Z",
            #     "to_datetime":"2023-06-15T15:00:00Z",
            #     "bars": json_obj["result"],
            #     "ticker":"1m"
            # })
            #a=a+result+"\n"
            with open(outfilename,"a") as ww:
                ww.write(result)
                ww.close()
    except Exception as e:
        print(e)
        break
    
if a != "": 
    generate_csv(a)



