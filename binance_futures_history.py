import pandas as pd 
import requests 
import hashlib
import hmac
import time
from datetime import datetime, timedelta, date
from requests.exceptions import ConnectTimeout
from utility import convert_to_unix, convert_timestamp_to_date


def binance_closed_pnl (bin_api_key, bin_secret_key, unix_start , unix_end):
    
    base_url = 'https://fapi.binance.com'
    
    try:

        timestamp = int(time.time() * 1000)
        limit = 1000
        params = f'timestamp={timestamp}&limit={limit}&startTime={unix_start}&endTime={unix_end}'

        signature = hmac.new(bin_secret_key.encode('utf-8'), params.encode('utf-8'), hashlib.sha256).hexdigest()

        headers = {
            'X-MBX-APIKEY': bin_api_key
        }
    
        url = f"{base_url}/fapi/v1/userTrades?{params}&signature={signature}"
    
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Failed with status code {response.status_code}.")
            
    except ConnectTimeout:
        print(f"Binance closed Attempt failed due to connection timeout")


def parse_bin_closed (bin_api_key, bin_secret_key, unix_start, unix_end):
    bin_closed_data = []
    bin_closed_pnl = binance_closed_pnl(bin_api_key, bin_secret_key, unix_start, unix_end)

    print('binance_futures_raw')
    print(bin_closed_pnl)

    for order in bin_closed_pnl:
        unix_time = order.get('time')
        realizedPnl = order.get('realizedPnl')
        side = order.get('side')

        # Formatting
        if side.upper() == 'SELL':
            side = "Sell"
        elif side.upper() == 'BUY':
            side = "Buy"

        # Calculations
        price = float(order.get('price'))
        order_qty = float(order.get('qty'))

        order_data = {
            'date': convert_timestamp_to_date(unix_time),
            'orderId': order.get('orderId'),
            'symbol': order.get('symbol'),
            'side': side,
            'invested_value': order.get('execValue'),
            'exchange': 'binance_perps',
            'execPrice': order.get('execPrice'),
            'qty': order.get('orderQty'),
            'rPnL': realizedPnl
        }

        bin_closed_data.append(order_data)
    
    df_bin_closed = pd.DataFrame(bin_closed_data)

    return df_bin_closed


def loop_bin_closed (bin_api_key, bin_secret_key, start_time, end_time):
    if isinstance(start_time, str):
        start_time = datetime.strptime(start_time, '%Y-%m-%d')
    if isinstance(end_time, str):
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
        df_bin_closed_weekly = parse_bin_closed(bin_api_key, bin_secret_key, unix_start, unix_end)
        result_df = pd.concat([result_df, df_bin_closed_weekly], ignore_index=True)
        
        current_start_time = current_end_time 
    
    return result_df

# Master
def get_binance_futures_history(bin_api_key, bin_secret_key, start_date, end_date):
    df_futures_history = loop_bin_closed (bin_api_key, bin_secret_key, start_date, end_date)

    return df_futures_history