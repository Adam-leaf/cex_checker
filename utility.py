import math
import requests
import uuid
from datetime import datetime

def convert_to_unix(date_input):
    
    if isinstance(date_input, str):
        date_obj = datetime.strptime(date_input, '%Y-%m-%d')
    elif isinstance(date_input, datetime):
        date_obj = date_input
    else:
        raise ValueError("Input should be a date string or a datetime object")
    
    # Convert the datetime object to a Unix timestamp (in seconds) and then to milliseconds
    timestamp_ms = date_obj.timestamp() * 1000
    no_dec_timestamp = math.trunc(timestamp_ms)
    return no_dec_timestamp

def convert_timestamp_to_date(timestamp_ms_str):
    # Convert the string timestamp to an integer
    timestamp_ms = int(timestamp_ms_str)
    
    # Convert the timestamp from milliseconds to seconds
    timestamp_sec = timestamp_ms / 1000
    
    # Convert the timestamp to a datetime object
    date_time = datetime.fromtimestamp(timestamp_sec)
    
    # Format the datetime object to a date-only string
    date_only = date_time.strftime('%Y-%m-%d')
    
    return date_only

def generate_custom_uuid(all_unique=False, *args):

    # Combine all arguments into a single string
    combined = '_'.join(str(arg) for arg in args)
    
    # If all_unique is True, add current timestamp to ensure uniqueness
    if all_unique:
        combined += datetime.now().isoformat()
    
    # Use the combined string to create a UUID
    return str(uuid.uuid5(uuid.NAMESPACE_OID, combined))

def save_dataframe_to_csv(dataframe, file_path):
    try:
        dataframe.to_csv(file_path, index=False)
        print(f"DataFrame successfully saved to {file_path}")
    except Exception as e:
        print(f"An error occurred while saving the DataFrame to CSV: {e}")


# Bybit
def get_bybit_price(asset):

    renamed = {'MATIC': 'POL'}
    
    # Check if the asset needs to be renamed
    asset = renamed.get(asset, asset)

    # Stablecoins
    if asset in ['USDT', 'USDC', 'BUSD']:
        return 1.0
    
    # If not, fetch the price from the API
    url = f'https://api.bybit.com/v5/market/tickers?category=spot&symbol={asset}USDT'
    response = requests.get(url)
    data = response.json()

    # Convert price to float and cache it
    result = data.get('result', {})
    list_data = result.get('list', [])

    if list_data:
        lastPrice = list_data[0].get('lastPrice')
        if lastPrice is not None:
            return float(lastPrice)

    # If price cannot be found
    print(f"Price for {asset} could not be found.")
    return 0
    

def get_bybit_hist_price(asset, timestamp):
    category = 'spot'

    # Stablecoins
    if asset in ['USDT', 'USDC', 'BUSD']:
        return 1.0

    query = f"symbol={asset}USDT&interval=1&category={category}&start={timestamp}&end={timestamp}"  # 1m interval
    url = f"https://api.bybit.com/v5/market/kline?{query}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print(data)
        
        if data.get("result", {}).get("list"):  # Check if data exists
            closing_price = float(data["result"]["list"][0][4])
            return closing_price
        else:  # If data is empty or doesn't exist for the timestamp
            print(f"No data exists for {asset} at timestamp {timestamp}")
            return 0
    except requests.RequestException as e:
        print(f"Error fetching data for {asset} at timestamp {timestamp}: {e}")
        return 0
    except (ValueError, IndexError, KeyError) as e:
        print(f"Error processing data for {asset} at timestamp {timestamp}: {e}")
        return 0
    