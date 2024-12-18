from bybit_futures_history import get_bybit_futures_history
from bybit_spot_history import get_bybit_spot_history
from binance_futures_history import get_binance_futures_history
from utility import assign_time, save_dataframe_to_csv
from dotenv import load_dotenv
from pathlib import Path
import os
import csv
import json

load_dotenv()

def process_owners(owner):

    # acc_owners should be a list, ie: acc_owners = ['VKEE', 'J']
    pic = {
        "J": "Jansen",
        "VKEE": "Vkee",
        "JM": "Jomo",
        "JM2": "Jomo2",
    }

    print(f'Checking Owner: {owner}')
    
    bb_api_key = os.getenv(f'{owner}_BYBIT_API_KEY', 'none')
    bb_secret_key = os.getenv(f'{owner}_BYBIT_SECRET_KEY', 'none')
    bin_api_key = os.getenv(f'{owner}_BIN_API_KEY', 'none')
    bin_secret_key = os.getenv(f'{owner}_BIN_SECRET_KEY', 'none')

    owner_data = {
        "bybit_api_key": bb_api_key,
        "bybit_secret_key": bb_secret_key,
        "bin_api_key": bin_api_key,
        "bin_secret_key": bin_secret_key,
        "pic": pic.get(owner),
    }

    return owner_data


def save_bybit_records(acc_owners, mode):
    """
        Save trading history for a given owner within a specified date range.

        :param acc_owners: a list of owners as seen in the .env file
        :param mode: Either "Weekly" or "Full"
    """

    start_date, end_date = assign_time(mode) # "Weekly"/ "Yearly" / "Full"
    print('bybit')
    print(start_date)
    print(end_date)

    for owner in acc_owners:

        owner_data = process_owners(owner)
        # Check if Bybit API key or secret key is "none"
        if owner_data['bybit_api_key'] == 'none' or owner_data['bybit_secret_key'] == 'none':
            print(f"Skipping owner {owner} due to missing Bybit API credentials")
            continue

        df_bybit_closed_futures = get_bybit_futures_history(owner_data['bybit_api_key'], owner_data['bybit_secret_key'], start_date, end_date)
        df_bybit_trades_spot = get_bybit_spot_history(owner_data['bybit_api_key'], owner_data['bybit_secret_key'], start_date, end_date)

        save_dataframe_to_csv(df_bybit_closed_futures,f'records/bybit_futures_{owner}.csv')
        save_dataframe_to_csv(df_bybit_trades_spot,f'records/bybit_spot_{owner}.csv')


def save_binance_records(acc_owners, mode):
    start_date, end_date = assign_time(mode) # "Weekly"/ "Yearly" / "Full"
    print('binance')
    print(start_date)
    print(end_date)

    for owner in acc_owners:
        owner_data = process_owners(owner)

        if owner_data['bin_api_key'] == 'none' or owner_data['bin_secret_key'] == 'none':
            print(f"Skipping owner {owner} due to missing Binance API credentials")
            continue
            
        df_binance_closed_futures = get_binance_futures_history(owner_data['bin_api_key'], owner_data['bin_secret_key'], start_date, end_date)
        save_dataframe_to_csv(df_binance_closed_futures,f'records/binance_futures_{owner}.csv')
    

def clean_bybit_csv():
    # Input path (where CSV files are)
    input_path = Path('records')
    # Output path (where JSON files will be saved)
    output_path = Path('records/cleaned')
    
    # Create cleaned directory if it doesn't exist
    output_path.mkdir(exist_ok=True)
    
    for filename in os.listdir(input_path):
        # Skip files that don't match our patterns
        if not filename.endswith('.csv'):
            continue
            
        if filename.startswith('bybit_futures_'):
            type_key = 'symbol'
            trade_type = 'futures'
        elif filename.startswith('bybit_spot_'):
            type_key = 'position'
            trade_type = 'spot'
        else:
            continue
        
        # Get owner name and create paths
        owner = filename.split('_')[-1].replace('.csv', '')
        input_file = input_path / filename
        output_file = output_path / f'cleaned_bybit_{trade_type}_{owner}.json'
        
        # Process the file
        results = {}
        with open(input_file, 'r') as file:
            for row in csv.DictReader(file):
                key = row[type_key]
                if key not in results:
                    results[key] = []
                results[key].append(row)
        
        # Save results
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)


def clean_binance_csv():
    # Input path (where CSV files are)
    input_path = Path('records')
    # Output path (where JSON files will be saved)
    output_path = Path('records/cleaned')
    
    # Create cleaned directory if it doesn't exist
    output_path.mkdir(exist_ok=True)
    
    for filename in os.listdir(input_path):
        # Only process binance files
        if not filename.startswith('binance_futures_') or not filename.endswith('.csv'):
            continue
        
        # Get owner name and create paths
        owner = filename.split('_')[-1].replace('.csv', '')
        input_file = input_path / filename
        output_file = output_path / f'cleaned_binance_futures_{owner}.json'
        
        # Process the file
        results = {}
        with open(input_file, 'r') as file:
            for row in csv.DictReader(file):
                symbol = row['symbol']
                if symbol not in results:
                    results[symbol] = []
                results[symbol].append(row)
        
        # Save results
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)

if __name__ == '__main__' : 
    acc_owners = ['J', 'VKEE', 'JM', 'JM2']
    #save_bybit_records(acc_owners, "Full")
    #save_binance_records(acc_owners, "Full")
    clean_bybit_csv()
    clean_binance_csv()