import pandas as pd
import requests
import time
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
import gc
from sklearn.preprocessing import LabelEncoder


url= "https://tl.net/tlpd/korean/games#tblt-4933-1-1-DESC"
browser = webdriver.Chrome()
browser.get(url)
time.sleep(5)

html = browser.page_source
soup = BeautifulSoup(html, 'html.parser')
gamesinfo = soup.find('table', id="tblt_table")

table_titles_list = gamesinfo.find_all('th')
table_titles_clean = [title.text.strip().lower() for title in table_titles_list][1:-1]
table_titles_add = ['league_id', 'map_id', 'winner_id', 'loser_id']
table_titles_clean.extend(table_titles_add)

df = pd.DataFrame(columns = table_titles_clean)

## this is completely stupid, but good practice. I find the total page numbers for the table and use this as the number to loop. 
## Totally unnecessary since the table will never be updated, but why not? I guess it's dumb because it was just as manual as just setting n = 1083.
pagechecker = soup.find_all('span', {'style' : 'padding:0; margin:0; font-size:8pt'})
pagetotal = [int(data.text.split(' ')[3]) for data in pagechecker][0]


pagecount = 0

for n in range(pagetotal):
    
    curr_page = n + 1
    
    url= f"https://tl.net/tlpd/korean/games#tblt-4933-{curr_page}-1-DESC"
    
    time.sleep(30)
    browser = webdriver.Chrome()
    browser.get(url)
    time.sleep(5)

    html = browser.page_source
    soup = BeautifulSoup(html, 'html.parser')
    gamesinfo = soup.find('table', id="tblt_table")

    column_data = gamesinfo.find_all('tr')

    for row in column_data[1:]:
        row_data = row.find_all('td')
        indiv_row_data = [data.text.strip() for data in row_data][1:-1]
        indiv_row_data[0] = datetime.strptime(indiv_row_data[0],'%y-%m-%d').strftime('%m/%d/%Y')
        
        ref_data = row.find_all('a')
        if "map" in [data.attrs.get('href') for data in ref_data[1:5]][1]:
            ref_row_data = [data.attrs.get('href').split('/', 4)[4].split('_')[0] for data in ref_data[1:5]]
        else:
            ref_row_data = [data.attrs.get('href').split('/', 4)[4].split('_')[0] for data in ref_data[1:4]]
            ref_row_data.insert(1, np.nan)
            
        indiv_row_data.extend(ref_row_data)

        length = len(df)
        df.loc[length] = indiv_row_data
    
    pagecount += 1

    ## Sometimes may fail due to page not loading; add failsafe if needed

    print(f"Page {curr_page} added to dataframe...")


if pagecount != pagetotal:
    print(f"Warning: Only {pagecount} of {pagetotal} pages has been loaded to the dataframe.")
else:
    print(f"Success: All {pagecount} of {pagetotal} pages has been loaded to the dataframe.")

df.to_csv('../data/games.csv', index=False)


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

for row in df_players.itertuples():
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
    print(f"Player {current_row} of {rowcount} added to dataframe...")    

if current_row != rowcount:
    print(f"Warning: Only {current_row} of {rowcount} players' races has been loaded to the dataframe.")
else:
    print(f"Success: All {current_row} of {rowcount} players' races has been loaded to the dataframe.")

df_players.to_csv('../data/players.csv', index=False)