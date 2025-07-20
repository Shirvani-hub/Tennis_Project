
import pandas as pd 

away_team=pd.read_csv("merged_away_team_data.csv")
home_team=pd.read_csv("merged_home_team_data.csv")
all_players=pd.concat([away_team["height"],home_team["height"]],axis=0)

print(all_players.dropna().unique().mean())
    
