import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_USER = os.getenv("DB_USER_PROD")
DB_PASS = os.getenv("DB_PASS_PROD")
DB_HOST = os.getenv("DB_HOST_PRD")
DB_PORT = os.getenv("DB_PORT_PROD")
DB_NAME = os.getenv("DB_NAME_PROD")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)


commodities = {'CL=F', 'GC=F', 'SI=F'}

# Função para buscar dados de uma commodity
def search_commodities_data(ticker_symbol, lookback_days=15):
    """
    Busca os dados históricos de uma commodity e retorna um dataframe.

    Parameters
    ----------
    ticker_symbol : str
        Símbolo da commodity.
    lookback_days : int, optional
        Número de dias para buscar histórico. Padrão é 15.

    Returns
    -------
    pandas.DataFrame
        Dataframe com os dados históricos da commodity.
    """
    ticker = yf.Ticker(ticker_symbol)
    data = ticker.history(period=f'{lookback_days+1}d', interval='1d')[['Close']]  # Obtém pelo menos 1 dia extra para segurança
    data = data.reset_index()  # Reseta o índice para que a data seja uma coluna
    data['Close'] = data['Close'].round(2)  # Arredonda os preços para 2 casas decimais
    data['symbol'] = ticker_symbol  # Adiciona o símbolo como string
    data.rename(columns={'Date': 'closeddate'}, inplace=True)  # Renomeia a coluna de data

    # Filtrar apenas os últimos 'lookback_days'
    data = data.tail(lookback_days)
    return data

# Função para buscar dados de todas as commodities
def search_all_commodities():
    """
    Busca os dados históricos de todas as commodities e concatena em um único dataframe.
    
    Returns
    -------
    pandas.DataFrame
        Dataframe com os dados históricos de todas as commodities.
    """
    all_data = []
    for symbol in commodities:
        data = search_commodities_data(symbol)
        all_data.append(data)
    return pd.concat(all_data, ignore_index=True)

# Função para carregar os dados no banco
def load_data(df, schema='public'):
    """
    Carrega os dados em um banco PostgreSQL.
    
    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe com os dados a serem carregados
    schema : str, optional
        Nome do schema do banco de dados, default é 'public'
    """
    
    df.to_sql('commodities_data', con=engine, schema=schema, index=True, if_exists='replace')

if __name__ == '__main__':
    commodities_data = search_all_commodities()
    load_data(commodities_data)