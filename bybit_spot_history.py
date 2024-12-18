
# Base Imports
import hashlib
import hmac
import time
from datetime import timedelta
import urllib.parse
import json
from requests.exceptions import ConnectTimeout, RequestException

# External Imports
from utility import convert_timestamp_to_date, generate_custom_uuid, convert_to_unix
import pandas as pd
import requests

def get_bybit_trade_history(bb_api_key, bb_secret_key, category, start_time, end_time, cursor, max_retries=3, delay=5):
    url = "https://api.bybit.com/v5/execution/list"
    parse_cursor = urllib.parse.quote(cursor)

    parameters = {
        "category": category,
        "startTime": start_time,
        "endTime": end_time,
        "cursor": cursor
    }
    
    for attempt in range(max_retries):
        try:
            timestamp = str(int(time.time() * 1000))
            queryString = f"category={category}&startTime={start_time}&endTime={end_time}&cursor={parse_cursor}"
            param_str = f'{timestamp}{bb_api_key}{queryString}'
            signature = hmac.new(bb_secret_key.encode('utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()

            headers = {
                "accept": "application/json",
                'X-BAPI-SIGN': signature,
                'X-BAPI-API-KEY': bb_api_key,
                'X-BAPI-TIMESTAMP': timestamp,
            }
            
            response = requests.get(url, headers=headers, params=parameters, timeout=10)
            
            if response.status_code == 200:
                return {
                    "statusCode": 200,
                    "body": response.json()
                }
            
            elif response.status_code == 429:  # Rate limit exceeded
                print(f"Rate limit exceeded. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"Request failed with status code {response.status_code}. Retrying...")
                time.sleep(delay)
        
        except (ConnectTimeout, RequestException) as e:
            if attempt < max_retries - 1:
                print(f"Request failed: {str(e)}. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                return {
                    "statusCode": 500,
                    "body": json.dumps({
                        "message": f"Request failed after {max_retries} attempts: {str(e)}"
                    })
                }

    return {
        "statusCode": 500,
        "body": json.dumps({
            "message": f"Request failed after {max_retries} attempts"
        })
    }

def loop_get_bybit_history(bb_api_key, bb_secret_key, category, start_date, end_date):
    unix_start = convert_to_unix(start_date)
    unix_end = convert_to_unix(end_date)
    print(f"Bybit: Full unix range {unix_start}, {unix_end}")

    current_start_time = start_date
    trade_history_full = []

    while current_start_time < end_date:
        current_end_time = min(current_start_time + timedelta(days=1), end_date)

        unix_start = convert_to_unix(current_start_time)
        unix_end = convert_to_unix(current_end_time)

        print(f"Starting at {unix_start}, ending at {unix_end}") 

        cursor = ""
        trade_history_in_range = []

        while True:
            raw_history = get_bybit_trade_history(bb_api_key, bb_secret_key, category, unix_start, unix_end, cursor)

            if raw_history['statusCode'] != 200:
                print(f"Error fetching data: {raw_history['body']}")
                break

            result = raw_history.get('body', {}).get('result', {})
            cursor = result.get('nextPageCursor')

            history_list = result.get('list', [])
            trade_history_in_range.extend(history_list)

            if not cursor:
                break

        trade_history_full.extend(trade_history_in_range)
        current_start_time = current_end_time 

    return trade_history_full

def parse_bybit_hist(bybit_trade_history):

    bybit_orders = []

    for trade in bybit_trade_history:
        
        execValue = trade.get('execPrice') # Reconfirm if execPrice or execValue will give in USD terms
        execQty = trade.get('execQty')
        usd_value = float(execValue) * float(execQty)
        date = trade.get('execTime')
        trade_id = trade.get('execId')
        symbol = trade.get('symbol')
        action = trade.get('side')

        order = {
            'date': convert_timestamp_to_date(date),
            'trade_id': trade_id,
            'position': symbol,
            'action': action,
            'exchange': 'bybit_spot',
            'exec_qty': execQty,
            'exec_price': execValue,
            'usd_value': usd_value
        }
    
        bybit_orders.append(order)
    
    df_bybit_orders = pd.DataFrame(bybit_orders)
    return df_bybit_orders

# Master
def get_bybit_spot_history(bb_api_key, bb_secret_key, start_date, end_date):
    raw_result = loop_get_bybit_history(bb_api_key, bb_secret_key, 'spot', start_date, end_date)
    df_parsed_hist = parse_bybit_hist(raw_result)

    return df_parsed_hist