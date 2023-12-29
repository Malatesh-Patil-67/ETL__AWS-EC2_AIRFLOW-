import yfinance as yf
import pandas as pd
import datetime

def fetch_stock_data(tickers, start_date, end_date):
    data = yf.download(tickers, start=start_date, end=end_date, group_by='ticker')
    
    # Handle delisted stocks
    data = data.dropna(how='all', axis=1)
    
    return data

def run_stock_etl():
    # Define a list of stock symbols
    stock_symbols = ['AAPL', 'GOOGL', 'AMZN', 'MSFT', 'TSLA', 'NFLX', 'NVDA', 'BA', 'DIS']

    # Define the date range
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=365)

    # Fetch stock data for multiple symbols
    stock_data = fetch_stock_data(stock_symbols, start_date, end_date)

    # Concatenate individual DataFrames into a single DataFrame
    all_stock_data = pd.concat([stock_data[symbol] for symbol in stock_symbols], axis=1, keys=stock_symbols)

    # Save consolidated stock data to a single CSV file
    all_stock_data.to_csv("s3://patil67bucket/all_stock_data.csv")

if __name__ == "__main__":
    run_stock_etl()
