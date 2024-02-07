#!/usr/bin/env python
# coding: utf-8


import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import LabelEncoder

df_games = pd.read_csv('../data/games.csv')

df_games = df_games[df_games["winner"].str.contains("\n") == False] # remove 2v2s
df_games = df_games[df_games["loser"].str.contains("\n") == False]

df_games['date'] = pd.to_datetime(df_games['date']).dt.strftime('%Y-%m-%d') #possibly remove this later, as the staging date needed to be converted but final scraper should pull dt.
df_games.index.name = 'MyIdx'
df_games = df_games.sort_values(by = ['date', 'MyIdx'], ascending = [True, False])
df_games = df_games.reset_index(drop=True)

## using label encoder to simplify elo tracking over time
## make sure to stack unique values in both winner and loser id so they match
label_encoder = LabelEncoder()
label_encoder.fit(df_games[['winner_id','loser_id']].stack().unique())
df_games['w_id_enc'] = label_encoder.transform(df_games['winner_id'])
df_games['l_id_enc'] = label_encoder.transform(df_games['loser_id'])

df_games['w_elo_pre'] = 0
df_games['w_elo_post'] = 0
df_games['l_elo_pre'] = 0
df_games['l_elo_post'] = 0

mean_elo = 1200
elo_width = 400

## We will use the 64/32 for k factor given the smaller number of games - https://en.wikipedia.org/wiki/Elo_rating_system#Most_accurate_K-factor
## The k-factor of 16 will need to be written in later.
k_factor_new = 64
k_factor_mid = 32
k_factor_exp = 16

mid_count = 20
exp_elo = 2400


## https://en.wikipedia.org/wiki/Elo_rating_system#Mathematical_details

def elo_updater(winner_elo, loser_elo, winner_k, loser_k):
    expected_win = expected_result(winner_elo, loser_elo)
    winner_change_in_elo = winner_k * (1-expected_win)
    loser_change_in_elo = loser_k * (1-expected_win)
    winner_elo += winner_change_in_elo
    loser_elo -= loser_change_in_elo
    return winner_elo, loser_elo

def expected_calculator(winner_elo, loser_elo):
    return 1.0/(1+10**((loser_elo - winner_elo)/elo_width))


## create current tracker for elo/games/wins/losses as well as over-time tracker for each player's encoded id

player_count = len(label_encoder.classes_)
curr_games = np.zeros(shape=(player_count))
curr_wins = np.zeros(shape=(player_count))
curr_losses = np.zeros(shape=(player_count))
curr_elos = np.ones(shape=(player_count)) * mean_elo
max_elos = np.ones(shape=(player_count)) * mean_elo
min_elos = np.ones(shape=(player_count)) * mean_elo
first_game = np.zeros(shape=(player_count), dtype='datetime64[D]')
last_game = np.zeros(shape=(player_count), dtype='datetime64[D]')

df_elos_series = pd.DataFrame(index=df_games.date.unique(), columns=range(player_count))

df_games_played_total = pd.DataFrame(index=df_games.date.unique(), columns=range(player_count))
df_games_played_total.iloc[:, :] = 0

df_games_played_daily = pd.DataFrame(index=df_games.date.unique(), columns=range(player_count))
df_games_played_daily.iloc[:, :] = 0

df_games_wins_total = pd.DataFrame(index=df_games.date.unique(), columns=range(player_count))
df_games_wins_total.iloc[:, :] = 0

df_games_wins_daily = pd.DataFrame(index=df_games.date.unique(), columns=range(player_count))
df_games_wins_daily.iloc[:, :] = 0

df_games_losses_total = pd.DataFrame(index=df_games.date.unique(), columns=range(player_count))
df_games_losses_total.iloc[:, :] = 0

df_games_losses_daily = pd.DataFrame(index=df_games.date.unique(), columns=range(player_count))
df_games_losses_daily.iloc[:, :] = 0


## loop to change elo over each game and add data at end of day

current_date = df_games.at[0, 'date']
for row in df_games.itertuples():
    
    # save elo from previous dates to new date
    if row.date != current_date:
        df_elos_series.loc[row.date, :] = df_elos_series.loc[current_date, :]
        df_games_played_total.loc[row.date, :] = df_games_played_total.loc[current_date, :]
        df_games_wins_total.loc[row.date, :] = df_games_wins_total.loc[current_date, :]
        df_games_losses_total.loc[row.date, :] = df_games_losses_total.loc[current_date, :]
        current_date = row.date
    
    # set row values and ids
    idx = row.Index
    winner_id = row.w_id_enc
    loser_id = row.l_id_enc
    
    # get current elo

    winner_elo_before = curr_elos[winner_id]
    loser_elo_before = curr_elos[loser_id]
    
    # get k_factor for players
    if curr_games[winner_id] >= 30:
        winner_k = k_factor_mid
    else:
        winner_k = k_factor_new
        
    if curr_games[loser_id] >= 30:
        loser_k = k_factor_mid
    else:
        loser_k = k_factor_new
    
    # update elo after game
    winner_elo_after, loser_elo_after = elo_updater(winner_elo_before, loser_elo_before, winner_k, loser_k)
        
    # saving elo values
    df_games.at[idx, 'w_elo_pre'] = winner_elo_before
    df_games.at[idx, 'l_elo_pre'] = loser_elo_before
    df_games.at[idx, 'w_elo_post'] = winner_elo_after
    df_games.at[idx, 'l_elo_post'] = loser_elo_after
    
    # Save to current data for elo and game count
    curr_elos[winner_id] = winner_elo_after
    curr_elos[loser_id] = loser_elo_after
    curr_games[winner_id] += 1
    curr_games[loser_id] += 1    
    curr_wins[winner_id] += 1
    curr_losses[loser_id] += 1     
    
    if first_game[winner_id] == np.datetime64('1970-01-01'):
        first_game[winner_id] = row.date
        last_game[winner_id] = row.date
    else:
        last_game[winner_id] = row.date
    
    if first_game[loser_id] == np.datetime64('1970-01-01'):
        first_game[loser_id] = row.date
        last_game[loser_id] = row.date
    else:
        last_game[loser_id] = row.date
    
    if winner_elo_after > max_elos[winner_id]:
        max_elos[winner_id] = winner_elo_after
    if loser_elo_after < min_elos[winner_id]:
        min_elos[loser_id] = loser_elo_after       
        
    # Save to series dataframe for elo and games
    today = row.date
    df_elos_series.at[today, winner_id] = winner_elo_after
    df_elos_series.at[today, loser_id] = loser_elo_after
    df_games_played_total.at[today, winner_id] += 1
    df_games_played_total.at[today, loser_id] += 1    
    df_games_wins_total.at[today, winner_id] += 1
    df_games_losses_total.at[today, loser_id] += 1
    df_games_played_daily.at[today, winner_id] += 1
    df_games_played_daily.at[today, loser_id] += 1    
    df_games_wins_daily.at[today, winner_id] += 1
    df_games_losses_daily.at[today, loser_id] += 1    
    
    

## creating new tables to be imported to postgres

## create player profile table -- add races later. concat winner and loser id

players_p1 = df_games.iloc[:, [3,7,9]]
players_p2 = df_games.iloc[:, [4,8,10]]
new_columns = ["player_name", "player_id", "player_id_enc"]
players_p1.columns = new_columns
players_p2.columns = new_columns
df_players = pd.concat([players_p1, players_p2], ignore_index=True).drop_duplicates()


## merge on final career data

df_players = pd.concat([players_p1, players_p2], ignore_index=True).drop_duplicates()
players_infoadd = pd.DataFrame(data=[first_game, last_game, curr_games, curr_wins, curr_losses, curr_elos, max_elos, min_elos]).T
players_infoadd.columns = ["first_game", "last_game", "career_games", "career_wins", "career_losses", "final_elo", "peak_elo", "min_elo"]
df_players = df_players.merge(players_infoadd, how = 'left', left_on = 'player_id_enc', right_index=True)
df_players = df_players.reset_index(drop=True)

## creating time series data

df_elos_series_melt = df_elos_series.melt(ignore_index=False).reset_index()
df_games_played_total_melt = df_games_played_total.melt(ignore_index=False).reset_index()
df_games_wins_total_melt = df_games_wins_total.melt(ignore_index=False).reset_index()
df_games_losses_total_melt = df_games_losses_total.melt(ignore_index=False).reset_index()
df_games_played_daily_melt = df_games_played_daily.melt(ignore_index=False).reset_index()
df_games_wins_daily_melt = df_games_wins_daily.melt(ignore_index=False).reset_index()
df_games_losses_daily_melt = df_games_losses_daily.melt(ignore_index=False).reset_index()

df_list = [df_elos_series_melt,
            df_games_played_total_melt['value'],
            df_games_wins_total_melt['value'],
            df_games_losses_total_melt['value'],
            df_games_played_daily_melt['value'],
            df_games_wins_daily_melt['value'],
            df_games_losses_daily_melt['value'],
          ]

df_series_data = pd.concat(df_list, axis='columns')
new_columns = ["date", "player_id_enc", "elo", "games_total", "wins_total", "losses_total", "games_played", "wins_played", "losses_played"]
df_series_data.columns = new_columns

## tables to be saved:

df_games.to_csv('../data/df_games.csv', index=False)
df_players.to_csv('../data/df_players.csv', index=False)
for idx, chunk in enumerate(np.array_split(df_series_data, 4)):
    chunk.to_csv(f'../data/df_series_{idx+1}.csv', index=False)
