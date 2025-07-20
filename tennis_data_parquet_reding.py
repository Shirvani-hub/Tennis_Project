
import pandas as pd
import pyarrow as pq
import pathlib
import os 
from pathlib import Path
import csv
import fnmatch


#----------------------------------
parquet_paths = os.getcwd()
pattern1 = "away_team_*.parquet"
pattern2 = "home_team_*.parquet"
pattern3 = "votes_*.parquet"
pattern4 = "power_*.parquet"
pattern5 = "statistics_*.parquet"
pattern6 = "pbp_*.parquet"
pattern7 = "odds_*.parquet"
pattern8 = "event_*.parquet"
pattern9 = "round_*.parquet"
pattern10 = "season_*.parquet"
pattern11 = "time_*.parquet"
pattern12 = "tournament_*.parquet"
pattern13 = "venue_*.parquet"
#-----------------------------------
away_dataframe = []
home_dataframe = []
votes_dataframe = []
power_dataframe = []
statistics_dataframe = []
pbp_dataframe = []
odds_dataframe = []
event_dataframe = []
round_dataframe = []
season_dataframe = []
time_dataframe = []
tournament_dataframe = []
venue_dataframe = []



#-------------------------------------------------
for root, dirs, files in os.walk(parquet_paths):
    for filename in fnmatch.filter(files, pattern1):
        full_path = os.path.join(root,filename)
        print(full_path)

        try:
            df = pd.read_parquet(full_path)
            away_dataframe.append(df)
            
        except Exception as e :
            print(f"Error in reading {filename}: {e}")


if away_dataframe:
    merged_df = pd.concat(away_dataframe, ignore_index=True)

    merged_df.to_csv("merged_away_team_data.csv", index=False)

    print("Data Saved!!!!")

else:
    print("Data Not saved!!!!")



   
#-------------------------------------------------

#-------------------------------------------------
for root, dirs, files in os.walk(parquet_paths):
    for filename in fnmatch.filter(files, pattern2):
        full_path = os.path.join(root,filename)
        print(full_path)

        try:
            df = pd.read_parquet(full_path)
            home_dataframe.append(df)
            
        except Exception as e :
            print(f"Error in reading {filename}: {e}")


if home_dataframe:
    merged_df = pd.concat(home_dataframe, ignore_index=True)

    merged_df.to_csv("merged_home_team_data.csv", index=False)

    print("Data Saved!!!!")

else:
    print("Data Not saved!!!!")



   
#-------------------------------------------------

#-------------------------------------------------
for root, dirs, files in os.walk(parquet_paths):
    for filename in fnmatch.filter(files, pattern3):
        full_path = os.path.join(root,filename)
        print(full_path)

        try:
            df = pd.read_parquet(full_path)
            votes_dataframe.append(df)
            
        except Exception as e :
            print(f"Error in reading {filename}: {e}")


if votes_dataframe:
    merged_df = pd.concat(votes_dataframe, ignore_index=True)

    merged_df.to_csv("merged_votes_data.csv", index=False)

    print("Data Saved!!!!")

else:
    print("Data Not saved!!!!")



   
#-------------------------------------------------

#-------------------------------------------------
for root, dirs, files in os.walk(parquet_paths):
    for filename in fnmatch.filter(files, pattern4):
        full_path = os.path.join(root,filename)
        print(full_path)

        try:
            df = pd.read_parquet(full_path)
            power_dataframe.append(df)
            
        except Exception as e :
            print(f"Error in reading {filename}: {e}")


if power_dataframe:
    merged_df = pd.concat(power_dataframe, ignore_index=True)

    merged_df.to_csv("merged_power_data.csv", index=False)

    print("Data Saved!!!!")

else:
    print("Data Not saved!!!!")



#-------------------------------------------------
#-------------------------------------------------
for root, dirs, files in os.walk(parquet_paths):
    for filename in fnmatch.filter(files, pattern5):
        full_path = os.path.join(root,filename)
        print(full_path)

        try:
            df = pd.read_parquet(full_path)
            statistics_dataframe.append(df)
            
        except Exception as e :
            print(f"Error in reading {filename}: {e}")


if statistics_dataframe:
    merged_df = pd.concat(statistics_dataframe, ignore_index=True)

    merged_df.to_csv("merged_statistics_data.csv", index=False)

    print("Data Saved!!!!")

else:
    print("Data Not saved!!!!")


   
#-------------------------------------------------

#-------------------------------------------------
for root, dirs, files in os.walk(parquet_paths):
    for filename in fnmatch.filter(files, pattern6):
        full_path = os.path.join(root,filename)
        print(full_path)

        try:
            df = pd.read_parquet(full_path)
            pbp_dataframe.append(df)
            
        except Exception as e :
            print(f"Error in reading {filename}: {e}")


if pbp_dataframe:
    merged_df = pd.concat(pbp_dataframe, ignore_index=True)

    merged_df.to_csv("merged_pbp_data.csv", index=False)

    print("Data Saved!!!!")

else:
    print("Data Not saved!!!!")



#-------------------------------------------------
#-------------------------------------------------
for root, dirs, files in os.walk(parquet_paths):
    for filename in fnmatch.filter(files, pattern7):
        full_path = os.path.join(root,filename)
        print(full_path)

        try:
            df = pd.read_parquet(full_path)
            odds_dataframe.append(df)
            
        except Exception as e :
            print(f"Error in reading {filename}: {e}")


if odds_dataframe:
    merged_df = pd.concat(odds_dataframe, ignore_index=True)

    merged_df.to_csv("merged_odds_data.csv", index=False)

    print("Data Saved!!!!")

else:
    print("Data Not saved!!!!")



   
#-------------------------------------------------

#-------------------------------------------------
for root, dirs, files in os.walk(parquet_paths):
    for filename in fnmatch.filter(files, pattern8):
        full_path = os.path.join(root,filename)
        print(full_path)

        try:
            df = pd.read_parquet(full_path)
            event_dataframe.append(df)
            
        except Exception as e :
            print(f"Error in reading {filename}: {e}")


if event_dataframe:
    merged_df = pd.concat(event_dataframe, ignore_index=True)

    merged_df.to_csv("merged_event_data.csv", index=False)

    print("Data Saved!!!!")

else:
    print("Data Not saved!!!!")



   
#-------------------------------------------------

#-------------------------------------------------
for root, dirs, files in os.walk(parquet_paths):
    for filename in fnmatch.filter(files, pattern9):
        full_path = os.path.join(root,filename)
        print(full_path)

        try:
            df = pd.read_parquet(full_path)
            round_dataframe.append(df)
            
        except Exception as e :
            print(f"Error in reading {filename}: {e}")


if round_dataframe:
    merged_df = pd.concat(round_dataframe, ignore_index=True)

    merged_df.to_csv("merged_round_data.csv", index=False)

    print("Data Saved!!!!")

else:
    print("Data Not saved!!!!")


   
#-------------------------------------------------

#-------------------------------------------------
for root, dirs, files in os.walk(parquet_paths):
    for filename in fnmatch.filter(files, pattern10):
        full_path = os.path.join(root,filename)
        print(full_path)

        try:
            df = pd.read_parquet(full_path)
            season_dataframe.append(df)
            
        except Exception as e :
            print(f"Error in reading {filename}: {e}")


if season_dataframe:
    merged_df = pd.concat(season_dataframe, ignore_index=True)

    merged_df.to_csv("merged_season_data.csv", index=False)

    print("Data Saved!!!!")

else:
    print("Data Not saved!!!!")



   
#-------------------------------------------------

#-------------------------------------------------
for root, dirs, files in os.walk(parquet_paths):
    for filename in fnmatch.filter(files, pattern11):
        full_path = os.path.join(root,filename)
        print(full_path)

        try:
            df = pd.read_parquet(full_path)
            time_dataframe.append(df)
         
        except Exception as e :
            print(f"Error in reading {filename}: {e}")


if time_dataframe:
    merged_df = pd.concat(time_dataframe, ignore_index=True)

    merged_df.to_csv("merged_time_data.csv", index=False)

    print("Data Saved!!!!")

else:
    print("Data Not saved!!!!")



   
#-------------------------------------------------

#-------------------------------------------------
for root, dirs, files in os.walk(parquet_paths):
    for filename in fnmatch.filter(files, pattern12):
        full_path = os.path.join(root,filename)
        print(full_path)

        try:
            df = pd.read_parquet(full_path)
            tournament_dataframe.append(df)
            
        except Exception as e :
            print(f"Error in reading {filename}: {e}")


if tournament_dataframe:
    merged_df = pd.concat(tournament_dataframe, ignore_index=True)

    merged_df.to_csv("merged_tournament_data.csv", index=False)

    print("Data Saved!!!!")

else:
    print("Data Not saved!!!!")


   
#-------------------------------------------------

#-------------------------------------------------
for root, dirs, files in os.walk(parquet_paths):
    for filename in fnmatch.filter(files, pattern13):
        full_path = os.path.join(root,filename)
        print(full_path)

        try:
            df = pd.read_parquet(full_path)
            venue_dataframe.append(df)
            
        except Exception as e :
            print(f"Error in reading {filename}: {e}")


if venue_dataframe:
    merged_df = pd.concat(venue_dataframe, ignore_index=True)

    merged_df.to_csv("merged_venue_data.csv", index=False)

    print("Data Saved!!!!")

else:
    print("Data Not saved!!!!")


   
#-------------------------------------------------


