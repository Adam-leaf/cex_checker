from bybit_futures_history import get_bybit_futures_history
from bybit_spot_history import get_bybit_spot_history
from utility import assign_time, save_dataframe_to_csv
from dotenv import load_dotenv
import os

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

        save_dataframe_to_csv(df_bybit_closed_futures,f'records/bybit_futures_{owner}')
        save_dataframe_to_csv(df_bybit_trades_spot,f'records/bybit_spot_{owner}')


if __name__ == '__main__' : 
    acc_owners = ['J', 'VKEE', 'JM', 'JM2']
    save_bybit_records(acc_owners, "Full")