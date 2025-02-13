import pandas as pd
import requests
import time
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
import gc
from sklearn.preprocessing import LabelEncoder

df_games = pd.read_csv('../data/games.csv')
df_games = df_games[df_games["winner"].str.contains("\n") == False] # remove 2v2s
df_games = df_games[df_games["loser"].str.contains("\n") == False]

players_p1 = df_games.iloc[:, [3,7]]
players_p2 = df_games.iloc[:, [4,8]]
new_columns = ["player_name", "player_id"]
players_p1.columns = new_columns
players_p2.columns = new_columns
df_players = pd.concat([players_p1, players_p2], ignore_index=True).drop_duplicates()

label_encoder = LabelEncoder()
label_encoder.fit(df_players[['player_id']].stack().unique())
df_players['player_id_enc'] = label_encoder.transform(df_players['player_id'])
df_players = df_players.sort_values(by = 'player_id_enc', ascending = True)
df_players = df_players.reset_index(drop=True)
df_players = df_players.assign(player_race="")

rowcount = df_players.shape[0]
current_row = 0
player_added = 0
errors_on_add = []

for row in df_players.itertuples():
    try:
        url= f"https://tl.net/tlpd/korean/players/{row.player_id}_{row.player_name}"

        time.sleep(30)
        browser = webdriver.Chrome()
        browser.get(url)
        time.sleep(5)
        html = browser.page_source
        soup = BeautifulSoup(html, 'html.parser')
        race_info = soup.find('strong', string='Main Race').next_sibling.find_next(string=True)

        idx = row.Index
        df_players.at[idx, 'player_race'] = race_info
        current_row += 1
        player_added += 1
        print(f"Player {current_row} of {rowcount}, username {row.player_name} added to dataframe...")    
    except:
        print(f"Player {current_row} of {rowcount}, username {row.player_name}_{row.player_id} failed to be added...")
        errors_on_add.append(row.player_id)
        current_row += 1
        pass
        
if player_added != rowcount:
    print(f"Warning: Only {player_added} of {rowcount} players' races has been loaded to the dataframe.")
    print(f"Warning: Players {errors_on_add} have not been added. These will require manual fixes.")
    
else:
    print(f"Success: All {player_added} of {rowcount} players' races has been loaded to the dataframe.")

df_players.to_csv('../data/players.csv', index=False)