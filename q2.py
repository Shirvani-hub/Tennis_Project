
import pandas as pd 

away_team=pd.read_csv("merged_away_team_data.csv")
home_team=pd.read_csv("merged_home_team_data.csv")
away_team_mean = away_team.mean()
home_team_mean = home_team.mean()

height_mean = (away_team_mean + home_team_mean )/2
    
