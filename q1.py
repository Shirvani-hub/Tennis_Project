

import pandas as pd 
import csv
import sys


away_team=pd.read_csv("merged_away_team_data.csv")
home_team=pd.read_csv("merged_home_team_data.csv")
all_players=pd.concat([away_team["player_id"],home_team["player_id"]],axis=0)
print("The number of tennis players in the dataset is: ", len(all_players.dropna().unique()))
