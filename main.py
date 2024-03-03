import os
from dotenv import load_dotenv
import soccerdata as sd
import polars as pl
import polars.selectors as cs
from pymongo import MongoClient

load_dotenv()

stat_types_teams = ["standard", "shooting", "passing", "defense", "possession", "misc"]
stat_types_players = ["standard", "shooting", "passing", "defense", "playing_time", "keeper"]

fbref = sd.FBref(leagues="ENG-Premier League", seasons="2023-24", no_cache=True, no_store=True)

teams_stats = pl.DataFrame()
for stat_type in stat_types_teams:
  df = pl.from_pandas(fbref.read_team_season_stats(stat_type = stat_type).reset_index().drop(["league", "season", "url"], axis=1))
  splitted_column_names = [column_name.translate(str.maketrans({'(': '', ')': '', "'": ""})).split(', ') for column_name in df.columns]
  df.columns = ["-".join(splitted_column_name) if splitted_column_name[1] else splitted_column_name[0] for splitted_column_name in splitted_column_names]
  teams_stats = teams_stats.with_columns(team=pl.Series(df.select(pl.col("team")).to_series()))
  teams_stats = teams_stats.join(df, how="inner", on="team")
  teams_stats = teams_stats.select(~cs.ends_with("_right"))
  
players_stats = pl.DataFrame()
for i, stat_type in enumerate(stat_types_players):
  df = pl.from_pandas(fbref.read_player_season_stats(stat_type = stat_type).reset_index().drop(["league", "season"], axis=1))
  splitted_column_names = [column_name.translate(str.maketrans({'(': '', ')': '', "'": ""})).split(', ') for column_name in df.columns]
  df.columns = ["-".join(splitted_column_name) if splitted_column_name[1] else splitted_column_name[0] for splitted_column_name in splitted_column_names]
  if i == 0:
    players_stats = players_stats.with_columns(player=pl.Series(df.select(pl.col("player")).to_series()), team=pl.Series(df.select(pl.col("team")).to_series()))
  players_stats = players_stats.join(df, how="left", on=["player", "team"])
  players_stats = players_stats.select(~cs.ends_with("_right"))
  
teams_dim = pl.DataFrame({
    "team": players_stats["team"].unique(),
    "team_id": list(range(len(players_stats["team"].unique())))
})

players_stats = players_stats.join(teams_dim, how="inner", on="team").drop("team")
teams_stats = teams_stats.join(teams_dim, how="inner", on="team").drop("team")

  
CONNECTION_STRING = f"mongodb+srv://{os.getenv('MONGO_DB_USERNAME')}:{os.getenv('MONGO_DB_PASSWORD')}@cluster0.0sqf03e.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

client = MongoClient(CONNECTION_STRING, )

try:
  client["Soccerstats"].drop_collection("FactPlayers")
  client["Soccerstats"].drop_collection("FactTeams")
  client["Soccerstats"].drop_collection("DimTeams")
except:
  pass

database = client['Soccerstats']


database["FactPlayers"].insert_many(players_stats.to_dicts())
database["FactTeams"].insert_many(teams_stats.to_dicts())
database["DimTeams"].insert_many(teams_dim.to_dicts())