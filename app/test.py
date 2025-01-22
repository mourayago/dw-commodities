import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd

dotenv_path = os.path.join("dw-commodities/src/", ".env")
load_dotenv(dotenv_path)

# Obter as variáveis do arquivo .env
DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

# Criar a URL de conexão do banco de dados
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME_PROD'),
    user=os.getenv('DB_USER_PROD'),
    password=os.getenv('DB_PASS_PROD'),
    host=os.getenv('DB_HOST_PROD'),
    port="5432"
)

print('connected')

df = pd.read_sql("SELECT * FROM public.dm_commodities", conn)	
print(df)