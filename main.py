import os
from dotenv import load_dotenv
import soccerdata as sd
import polars as pl
import polars.selectors as cs
from pymongo import MongoClient

load_dotenv()

stat_types = ["standard", "shooting", "passing", "defense", "possession", "misc"]
fbref = sd.FBref(leagues="ENG-Premier League", seasons="2023-24", no_cache=True, no_store=True)

stats = pl.DataFrame()
for stat_type in stat_types:
  df = pl.from_pandas(fbref.read_team_season_stats(stat_type = stat_type).reset_index().drop(["league", "season", "url"], axis=1))
  splitted_column_names = [column_name.translate(str.maketrans({'(': '', ')': '', "'": ""})).split(', ') for column_name in df.columns]
  df.columns = ["-".join(splitted_column_name) if splitted_column_name[1] else splitted_column_name[0] for splitted_column_name in splitted_column_names]
  stats = stats.with_columns(team=pl.Series(df.select(pl.col("team")).to_series()))
  stats = stats.join(df, how="inner", on="team")
  stats = stats.select(~cs.ends_with("_right"))
  
CONNECTION_STRING = f"mongodb+srv://{os.getenv("MONGO_DB_USERNAME")}:{os.getenv("MONGO_DB_PASSWORD")}@cluster0.0sqf03e.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
 
client = MongoClient(CONNECTION_STRING)

try:
  client["soccerstats"].drop_collection("soccerstats_items")
except:
  pass
 
database = client['soccerstats']

collection = database["soccerstats_items"]

collection.insert_many(stats.to_dicts())