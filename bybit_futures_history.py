import pandas as pd 
import requests
import hashlib
import hmac
import time
from datetime import datetime, timedelta
import urllib.parse
from requests.exceptions import ConnectTimeout
from utility import convert_to_unix, convert_timestamp_to_date


def bybit_closed_pnl(bb_api_key, bb_secret_key, category, start_time, end_time, cursor) : 

    url = "https://api.bybit.com/v5/position/closed-pnl"
    recv_window = '2000'
    limit = '200'
    parse_cursor = urllib.parse.quote(cursor)
    
    parameters = {
        "category" : category,
        "startTime" : start_time,
        "endTime": end_time,
        "limit": limit,
        "cursor": cursor,
    }
    
    try:
        timestamp = str(int(time.time() * 1000))
        queryString = f"category={category}&startTime={start_time}&endTime={end_time}&limit={limit}&cursor={parse_cursor}"
        param_str = f'{timestamp}{bb_api_key}{recv_window}{queryString}'
        signature = hmac.new(bb_secret_key.encode('utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()
    
        headers = {
            "accept": "application/json",
            'X-BAPI-SIGN': signature,
            'X-BAPI-API-KEY': bb_api_key,
            'X-BAPI-TIMESTAMP': timestamp,
            'X-BAPI-RECV-WINDOW': recv_window
        }

        response = requests.get(url, headers=headers, params = parameters)
        
        if response.status_code == 200 and response is not None:
                data = response.json()
                return data
        else:
            print(f"Attempt failed")

    except ConnectTimeout:
        print(f"Bybit Closed Attempt failed due to connection timeout")


def parse_bybit_closed(bb_api_key, bb_secret_key, category, unix_start, unix_end):
    
    cursor = ""
    df_all = pd.DataFrame()

    while True:

        bb_closed_data = []
        bb_closed_pnl = bybit_closed_pnl(bb_api_key, bb_secret_key, category, unix_start, unix_end, cursor)
    
        result = bb_closed_pnl.get('result', {})

        if not result:
            print(f"No closed positions from {unix_start} to {unix_end}")
            return pd.DataFrame()
    
        list = result.get('list')
        cursor = result.get('nextPageCursor')

        for item in list:
            symbol = item.get('symbol')
            updatedTime = item.get('updatedTime')
            date = convert_timestamp_to_date(updatedTime)
            closedPnl = item.get('closedPnl')
            action = "Long"
            
            order_data = {
                'date': date,
                'exchange_id': 'bybit_unified',
                'symbol': symbol,
                'action': action,
                'exchange': 'bybit_unified',
                'invested_value': '-', # Also not sure how to calc
                'uPnL' : '0',
                'rPnL': closedPnl
            }
            
            bb_closed_data.append(order_data)
        
        df_bb_closed = pd.DataFrame(bb_closed_data)
        df_all = pd.concat([df_all, df_bb_closed], ignore_index=True)

        if not cursor:
            break
        
    return df_all


def loop_get_bybit_closed(bb_api_key, bb_secret_key, category, start_time, end_time):
    
    start_time = datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.strptime(end_time, '%Y-%m-%d')
    
    # Current start time for the loop
    current_start_time = start_time
    
    result_df = pd.DataFrame()
    
    # Loop until current start time exceeds end time
    while current_start_time < end_time:
        current_end_time = min(current_start_time + timedelta(days=1), end_time)

        unix_start = convert_to_unix(current_start_time)
        unix_end = convert_to_unix(current_end_time)

        # Calls function that calls the api
        df_bb_closed_daily = parse_bybit_closed(bb_api_key, bb_secret_key, category, unix_start, unix_end)
        result_df = pd.concat([result_df, df_bb_closed_daily], ignore_index=True)
        
        current_start_time = current_end_time 
    
    return result_df

def get_bybit_futures_history(bb_api_key, bb_secret_key, start_date, end_date):
    df_futures_history = loop_get_bybit_closed(bb_api_key, bb_secret_key, 'linear', start_date, end_date)

    return df_futures_history