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

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Validate environment variables
def validate_environment():
    missing_vars = []
    if not API_KEY:
        missing_vars.append("ALPHA_VANTAGE_API_KEY")
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not S3_BUCKET_NAME:
        missing_vars.append("AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, or S3_BUCKET_NAME")

    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

# Initialize AWS S3 client
def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

# Fetch historical stock data for a given symbol and date range
def fetch_stock_data(symbol, start_date, end_date):
    ts = TimeSeries(key=API_KEY, output_format='pandas')
    try:
        data, _ = ts.get_daily(symbol=symbol, outputsize='full')
        data.index = pd.to_datetime(data.index)
        data = data.sort_index(ascending=True)
        return data.loc[start_date:end_date, '4. close']
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        return pd.Series()

# Create and annotate stock plot
def plot_stock_data(data):
    plt.figure(figsize=(14, 8))
    markers = ['o', 'v', '^', '<', '>', 's', 'p', 'D']  # Different markers for better distinction

    for idx, symbol in enumerate(data.columns):
        if not data[symbol].empty:
            plt.plot(data.index, data[symbol], label=symbol, marker=markers[idx % len(markers)], markersize=5)

    # Add grid, labels, and title
    plt.title('Historical Stock Prices (Last Year)', fontsize=16, fontweight='bold')
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Closing Price (USD)', fontsize=12)
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.legend(loc='upper left')

    # Annotate last value of each stock
    for symbol in data.columns:
        last_date = data[symbol].index[-1]
        last_price = data[symbol].iloc[-1]
        plt.annotate(f'{last_price:.2f}', xy=(last_date, last_price), xytext=(5, 0), textcoords='offset points')

    # Save plot to a buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return buf

# Upload plot to S3
def upload_to_s3(s3_client, buffer, filename):
    try:
        with buffer:
            s3_client.upload_fileobj(buffer, S3_BUCKET_NAME, filename)
        logging.info(f"Plot uploaded to S3: {filename}")
    except Exception as e:
        logging.error(f"Failed to upload to S3: {e}")

# Main function
def main():
    try:
        # Validate environment
        validate_environment()

        # Set date range
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        data = pd.DataFrame()
        s3_client = get_s3_client()
        symbols = ['JPM', 'BAC', 'C', 'WFC', 'GS', 'MS', 'BLK', 'BX']

        # Fetch stock data
        for symbol in symbols:
            stock_data = fetch_stock_data(symbol, start_date, end_date)
            if not stock_data.empty:
                data[symbol] = stock_data
                logging.info(f"Fetched data for {symbol}")
            else:
                logging.warning(f"No data for {symbol}")

            time.sleep(12)  # Alpha Vantage rate limit

        # Plot and upload if data exists
        if not data.empty:
            filename = f'stock_prices_{end_date}.png'
            buffer = plot_stock_data(data)
            upload_to_s3(s3_client, buffer, filename)
        else:
            logging.warning("No data to plot or upload.")

    except ValueError as ve:
        logging.error(f"Environment validation error: {ve}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == '__main__':
    main()
