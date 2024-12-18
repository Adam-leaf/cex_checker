import math
import requests
import uuid
import os
from datetime import datetime, timedelta

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
        # Create 'records' directory if it doesn't exist
        os.makedirs('records', exist_ok=True)
        
        # Save the dataframe
        dataframe.to_csv(file_path, index=False)
        print(f"DataFrame successfully saved to {file_path}")
    except Exception as e:
        print(f"An error occurred while saving the DataFrame to CSV: {e}")

def assign_time(mode):
    """
    Has 4 modes:
        1. Full - Start Date is 2 years ago from current time
        2. Weekly - Start Date is 1 week ago from current time
        3. Monthly - Start Date is 4 weeks ago from current time
        4. Yearly - Start Date is 1 year ago from current time
        5. Since2023 - Start Date is January 1, 2023
    """
    current_time_exact = datetime.now()

    # Convert the date back to a datetime object at midnight (00:00:00)
    current_date = datetime.combine(current_time_exact, datetime.min.time())

    end_date = current_date 

    # 4 Modes
    if mode == 'Full': 
        print('Getting data from current date to 2 years ago')
        start_date = end_date - timedelta(days=730)  # 2 years = 730 days
        
    elif mode == 'Weekly':
        print('Getting data from current date to 1 week ago')
        start_date = end_date - timedelta(weeks=1)

    elif mode == 'Monthly':
        print('Getting data from current date to 4 weeks ago')
        start_date = end_date - timedelta(weeks=4)
    
    elif mode == 'Yearly':
        print('Getting data from current date to 1 year ago')
        start_date = end_date - timedelta(years=1)

    elif mode == 'Since2023':
        print('Getting data from current date to January 1, 2023')
        start_date = datetime(2023, 1, 1)

    else:
        raise ValueError("Invalid mode. Choose 'Full', 'Weekly', 'Monthly', 'Yearly', or 'Since2023'.")

    return start_date, end_date

    