import os
import boto3
import pandas as pd
import matplotlib.pyplot as plt
from alpha_vantage.timeseries import TimeSeries
from datetime import datetime, timedelta
import io  
import time
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get Alpha Vantage API key and AWS credentials from environment variables
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Validate the environment variables
def validate_environment():
    if not API_KEY:
        raise ValueError("Alpha Vantage API key is missing. Set the ALPHA_VANTAGE_API_KEY environment variable.")
    if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME]):
        raise ValueError("AWS credentials or S3 bucket name are missing. Ensure AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_BUCKET_NAME are set.")

# Initialize AWS S3 client
def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

# Fetch historical stock data for a given symbol and date range
def get_stock_data(symbol, start_date, end_date):
    ts = TimeSeries(key=API_KEY, output_format='pandas')
    try:
        # Get full daily data
        data, _ = ts.get_daily(symbol=symbol, outputsize='full')
        
        # Ensure the index is a DatetimeIndex and sorted in ascending order
        data.index = pd.to_datetime(data.index)
        data = data.sort_index(ascending=True)
        
        # Filter data by the provided date range
        filtered_data = data.loc[start_date:end_date]
        return filtered_data['4. close']  # Return only closing prices
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        return pd.Series()

# Plot and save the stock prices to a buffer
def create_stock_plot(data):
    plt.figure(figsize=(12, 6))
    for symbol in data.columns:
        if not data[symbol].empty:
            plt.plot(data.index, data[symbol], label=symbol)
    
    plt.title('Historical Stock Prices of Major Banks and Hedge Funds')
    plt.xlabel('Date')
    plt.ylabel('Closing Price (USD)')
    plt.legend()
    plt.grid(True)

    # Save the plot to a bytes buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    return buf

# Upload the plot to an S3 bucket
def upload_to_s3(s3_client, buffer, filename):
    try:
        s3_client.upload_fileobj(buffer, S3_BUCKET_NAME, filename)
        logging.info(f"Plot successfully uploaded to S3: {filename}")
    except Exception as e:
        logging.error(f"Error uploading plot to S3: {e}")

# Main function to orchestrate data fetching, plotting, and uploading
def main():
    try:
        # Validate environment variables
        validate_environment()

        # Set date range for fetching data (past year)
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        data = pd.DataFrame()
        s3_client = get_s3_client()  # Initialize AWS S3 client once
        
        # List of stock symbols for major banks and hedge funds
        symbols = ['JPM', 'BAC', 'C', 'WFC', 'GS', 'MS', 'BLK', 'BX']

        # Fetch data for each stock symbol
        for symbol in symbols:
            stock_data = get_stock_data(symbol, start_date, end_date)
            if not stock_data.empty:
                data[symbol] = stock_data
                logging.info(f"Fetched data for {symbol}")
            else:
                logging.warning(f"No data available for {symbol}")
            
            # Respect Alpha Vantage's rate limit (5 requests per minute)
            time.sleep(12)  # Sleep for 12 seconds to avoid rate limit issues
        
        # If there is valid data, create the plot and upload to S3
        if not data.empty:
            filename = f'stock_prices_{end_date}.png'
            buffer = create_stock_plot(data)
            upload_to_s3(s3_client, buffer, filename)
        else:
            logging.warning("No data available to plot or upload.")
    
    except ValueError as ve:
        logging.error(f"Environment validation error: {ve}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    main()