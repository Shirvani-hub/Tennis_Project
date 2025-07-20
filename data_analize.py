

import pandas as pd 
import csv
import sys


Away_team=pd.read_csv("merged_away_team_data.csv")
home_team=pd.read_csv("merged_home_team_data.csv")
all_players=pd.concat([Away_team["player_id"],home_team["player_id"]],axis=0)
print("The number of tennis players in the dataset is: ", len(all_players.unique()))
print(all_players)