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
    scraper = params.scraper
    elo = params.elo

    load_dotenv(override=True)

    print(os.getenv('POSTGRES_USER'))
    print(os.getenv('POSTGRES_PASSWORD'))
    print(os.getenv('POSTGRES_HOST'))
    print(os.getenv('POSTGRES_PORT'))
    print(os.getenv('POSTGRES_DBNAME'))
    
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT')
    db_name = os.getenv('POSTGRES_DBNAME')

    games_table = 'df_games'
    players_table = 'df_players'
    series_table = 'df_series'

    csv_games = './data/df_games.csv'
    csv_players = './data/df_players.csv'
    csv_series = './data/df_series.csv'

    if scraper == 1:
        runpy.run_path('./elo/elo_calc.py') 
    
    if elo == 1:
        runpy.run_path('./scraper/scraper.py') 


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

    df_zones = pd.read_csv(csv_players)
    
    df_zones.head(n=0).to_sql(name = players_table, con = engine, if_exists = 'replace')
    df_zones.to_sql(name = players_table, con = engine, if_exists = 'append')

    print('Table file inserted.') 


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                        prog='ingester',
                        description='Ingest CSV data to Postgres')

    parser.add_argument('--scraper', help= 'Run scraper if needed')
    parser.add_argument('--elo', help= 'Run elo calculator if needed')

    args = parser.parse_args()

    main(args)


## desired script - 

