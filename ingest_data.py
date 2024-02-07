#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import psycopg2
import runpy
from sqlalchemy import create_engine
from time import time
from dotenv import load_dotenv
import argparse 
import os

def main(params):

    load_dotenv(override=True)

    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT')
    db_name = os.getenv('POSTGRES_DBNAME')

    github = os.getenv('GITHUB_NAME')
    repository = os.getenv('PROJECT_NAME')
    branch = os.getenv('BRANCH_NAME')

    data_url = f'https://raw.githubusercontent.com/{github}/{repository}/{branch}/data/'

    table_series = 'df_series'
    table_games = 'df_games'
    table_players = 'df_players'
    table_series_1 = 'df_series_1'
    table_series_2 = 'df_series_2'
    table_series_3 = 'df_series_3'
    table_series_4 = 'df_series_4'

    url_games = f'{data_url}{table_games}.csv'
    url_players = f'{data_url}{table_players}.csv'
    url_series_1 = f'{data_url}{table_series_1}.csv'
    url_series_2 = f'{data_url}{table_series_2}.csv'
    url_series_3 = f'{data_url}{table_series_3}.csv'
    url_series_4 = f'{data_url}{table_series_4}.csv'

    csv_games = f'{table_games}.csv'
    csv_players = f'{table_players}.csv'
    csv_series_1 = f'{table_series_1}.csv'
    csv_series_2 = f'{table_series_2}.csv'
    csv_series_3 = f'{table_series_3}.csv'
    csv_series_4 = f'{table_series_4}.csv'

    os.system(f"wget {url_games} -O {csv_games}")
    os.system(f"wget {url_players} -O {csv_players}")
    os.system(f"wget {url_series_1} -O {csv_series_1}")
    os.system(f"wget {url_series_2} -O {csv_series_2}")
    os.system(f"wget {url_series_3} -O {csv_series_3}")
    os.system(f"wget {url_series_4} -O {csv_series_4}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

    df_games = pd.read_csv(csv_games)
    df_games[["date"]] = df_games[["date"]].apply(pd.to_datetime.dt.date)
    df_games.head(n=0).to_sql(name = table_games, con = engine, if_exists = 'replace')
    df_games.to_sql(name = table_games, con = engine, if_exists = 'append')
    print('Games table inserted.') 

    df_players = pd.read_csv(csv_players)
    df_players[["first_game", "last_game"]] = df_players[["first_game", "last_game"]].apply(pd.to_datetime.dt.date)
    df_players.head(n=0).to_sql(name = table_players, con = engine, if_exists = 'replace')
    df_players.to_sql(name = table_players, con = engine, if_exists = 'append')
    print('Players table inserted.') 

    series_list = [csv_series_1, csv_series_2, csv_series_3, csv_series_4]
    df_series = pd.read_csv(csv_series_1)
    df_series[["date"]] = df_series[["date"]].apply(pd.to_datetime.dt.date)
    df_series.head(n=0).to_sql(name = table_series, con = engine, if_exists = 'replace')
    series_n = 1
    for i in series_list:
        print('Reading file %.d' % (series_n))
        df_series = pd.read_csv(i)
        df_series[["date"]] = df_series[["date"]].apply(pd.to_datetime.dt.date)
        df_series.to_sql(name = table_series, con = engine, if_exists = 'append')
        print('Series %.d inserted' % (series_n))
        series_n += 1

    print('Series table fully inserted.')    

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                        prog='ingester',
                        description='Ingest CSV data to Postgres')

    args = parser.parse_args()

    main(args)


## desired script - 

