import pandas as pd
import psycopg2
import runpy
from sqlalchemy import create_engine
from time import time
from dotenv import load_dotenv
import argparse 
import os


load_dotenv(override=True)

user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
host = os.getenv('POSTGRES_HOST')
port = os.getenv('POSTGRES_PORT')
db_name = os.getenv('POSTGRES_DBNAME')

github = os.getenv('GITHUB_NAME')
repository = os.getenv('PROJECT_NAME')
data_url = f'https://github.com/{github}/{repository}/blob/main/data/'


table_games = 'df_games'

url_games = f'{data_url}{table_games}.csv'

csv_games = f'{table_games}.csv'

os.system(f"wget {url_games} -O {csv_games}")

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

df_games = pd.read_csv(csv_games)
print(df_games)