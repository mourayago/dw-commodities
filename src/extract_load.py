import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv


commodities = {'CL=F', 'GC=F', 'SI=F'}

def search_commodities_data(ticker, period='5d',interval='1d'):
    ticker = yf.Ticker(ticker)
    data = ticker.history(period=period,interval=interval)[['Close']]
    data['symbol'] = ticker
    return data

def search_all_commodities():
    all_data = []
    for simbol in commodities:
        data = search_commodities_data(simbol)
        all_data.append(data)
    return pd.concat(all_data, ignore_index=True)

if __name__ == '__main__':
    commodities_data = search_all_commodities()
    print(commodities_data)