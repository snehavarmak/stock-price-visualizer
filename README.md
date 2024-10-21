# **Stock Price Visualizer with AWS S3 and Alpha Vantage**

This Python application fetches historical stock price data for major U.S. banks and hedge funds using the Alpha Vantage API. It visualizes the data in a plot, which is then uploaded to an AWS S3 bucket for easy access and storage. The script is designed to run periodically, providing an auto-updating data visualization of the stock prices.

## **Features**
- Fetches historical stock prices for major financial institutions.
- Visualizes the data using `matplotlib`.
- Uploads the generated plots to an AWS S3 bucket.
- Uses Alpha Vantage API to retrieve stock market data.
- Configurable date range (default is past 365 days).
- Logging for debugging and monitoring.

## **Stock Symbols Tracked**
The following stock symbols are tracked by default:
- JPMorgan Chase (JPM)
- Bank of America (BAC)
- Citigroup (C)
- Wells Fargo (WFC)
- Goldman Sachs (GS)
- Morgan Stanley (MS)
- BlackRock (BLK)
- Blackstone (BX)

---
![stock_prices_2024-10-21](https://github.com/user-attachments/assets/9acc9c3f-4b16-4941-b73b-d4db0e104ec9)

## **Prerequisites**
Ensure you have the following installed:
- Python 3.x
- [Alpha Vantage API Key](https://www.alphavantage.co/support/#api-key)
- AWS account with S3 bucket access
- Libraries: `boto3`, `pandas`, `matplotlib`, `alpha_vantage`

Install the required Python packages:

```bash
pip install boto3 pandas matplotlib alpha_vantage
```

---

## **Setup**

### **1. Clone the repository**

```bash
git clone https://github.com/your-username/stock-price-visualizer.git
cd stock-price-visualizer
```

### **2. Set up environment variables**

You need to set the following environment variables either in your terminal or in a `.env` file (optional, if you're using a dotenv package).

```bash
export ALPHA_VANTAGE_API_KEY=your_alpha_vantage_api_key
export AWS_ACCESS_KEY_ID=your_aws_access_key_id
export AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
export S3_BUCKET_NAME=your_s3_bucket_name
```

Alternatively, create a `.env` file in the project directory:

```
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_api_key
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
S3_BUCKET_NAME=your_s3_bucket_name
```

### **3. Run the script**

Once your environment variables are set up, you can run the script using:

```bash
python main.py
```

---

## **How It Works**

1. **Fetch Stock Data**: The script uses the Alpha Vantage API to fetch historical daily stock data for each of the symbols specified. It respects the API's rate limit by pausing between requests.
2. **Generate Plots**: The historical stock prices are plotted using `matplotlib`.
3. **Upload to S3**: The generated plot is saved to a buffer and uploaded to your specified AWS S3 bucket.
4. **Logging**: The script logs information about data fetching, plot generation, and any errors encountered.

---

## **AWS S3 Setup**
To allow the script to upload to AWS S3, ensure:
- You have an S3 bucket created.
- Your IAM user or role has the necessary permissions to upload objects to that bucket.

You can use the following policy for your IAM user:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::your-s3-bucket-name/*"
        }
    ]
}
```

---

## **Customization**

### **Modifying Symbols**
You can modify the list of stock symbols by changing the `symbols` list in `main.py`:

```python
symbols = ['JPM', 'BAC', 'C', 'WFC', 'GS', 'MS', 'BLK', 'BX']
```

### **Changing Date Range**
To fetch stock data for a different date range, modify the `start_date` and `end_date` in the `main()` function.

---

## **Error Handling**

The script includes error handling for:
- Missing or invalid API keys.
- AWS S3 upload failures.
- Alpha Vantage API rate limit handling (automatic delay between requests).
- Missing data for specific stock symbols.

Any errors will be logged, and the script will continue to fetch data for other symbols without interruption.

---
