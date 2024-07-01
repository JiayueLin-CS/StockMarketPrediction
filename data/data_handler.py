import os
from os import listdir
import sys
import pandas as pd
import yfinance as yf # conda install -c conda-forge yfinance
# from constants import STOCKS

STOCKS = [
    "AMZN", # Amazon
    "TSLA", # Tesla
    "GOOG", # Google
    "AAPL", # Apple
    "PFE",  # Pfizer
    "BA",   # Boeing
    "DIS",  # Disney
    "SBUX"  # Starbuck
]

class DataHandler:
    def __init__(self):
        self.dataset_daily  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datasets_daily")
        self.dataset_hourly = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datasets_hourly")

    def download_stock_dataset(self, period="max", interval='1d'):
        for stock_ticket in STOCKS:
            print(f"Retrieving data for {stock_ticket} [{interval}]...")
            stock_data = yf.download(tickers=stock_ticket, period=period, interval=interval)
            
            # Add label to dataset
            stock_data['Label'] = 0
            
            open_idx = list(stock_data.columns).index("Open")
            close_idx = list(stock_data.columns).index("Adj Close")
            label_idx = list(stock_data.columns).index("Label")
            
            for i in range(1, len(stock_data)):
                if stock_data.iloc[i, open_idx] > stock_data.iloc[i, close_idx]:
                    stock_data.iloc[i-1, label_idx] = -1
                elif stock_data.iloc[i, open_idx] < stock_data.iloc[i, close_idx]:
                    stock_data.iloc[i-1, label_idx] = 1
            
            if interval == '1d':
                stock_data.to_csv(os.path.join(self.dataset_daily, stock_ticket+"-Daily"), encoding='utf-8', sep="|")
            elif interval == '1h':
                stock_data.to_csv(os.path.join(self.dataset_hourly, stock_ticket+"-Hourly"), encoding='utf-8', sep="|")

    def get_stock_dataset(self, stock_ticket):
        csv_files = stock_ticket + "-Daily"
        if csv_files in listdir(self.dataset_daily):
            df = pd.read_csv(os.path.join(self.dataset_daily, csv_files), sep="|")
        else:
            print(f"Dataset for {stock_ticket} not found!")
                    
        return df
        
if __name__ == "__main__":
    dh = DataHandler()
    dh.download_stock_dataset(period='max', interval='1d')
    dh.download_stock_dataset(period='2y', interval='1h')
    # stock_ticket = dh.get_stock_dataset(stock_ticket="AAPL")
    # print(stock_ticket)