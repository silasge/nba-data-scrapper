import duckdb
import pandas as pd 
from prefect import flow, task
from nba_api.stats.endpoints import TeamGameLogs

from nba_data_scrapper.conn import connect_to_db
from nba_data_scrapper.utils import insert_df_in_db, flag_home_away


@task 
def get_game_logs(season, season_type):
    game_log = TeamGameLogs(
        season_nullable=season,
        season_type_nullable=season_type
    ).get_data_frames()[0]
    return game_log


@task 
def process_game_logs(game_log):
    game_log["GAME_DATE"] = pd.to_datetime(game_log["GAME_DATE"])
    game_log = game_log.sort_values("GAME_DATE")
    game_log["HOME_AWAY"] = game_log["MATCHUP"].apply(lambda x: flag_home_away(x))
    game_log["OPPONENT"] = game_log["MATCHUP"].str[-3:]
    
    cols_order = [
        "SEASON_YEAR",
        "SEASON_TYPE",
        "TEAM_ID",
        "TEAM_ABBREVIATION",
        "TEAM_NAME",
        "GAME_ID",
        "GAME_DATE",
        "OPPONENT",
        "HOME_AWAY",
        "WL",
        "MIN",
        "FGM",
        "FGA",
        "FG_PCT",
        "FG3M",
        "FG3A",
        "FG3_PCT",
        "FTM",
        "FTA",
        "FT_PCT",
        "OREB",
        "DREB",
        "REB",
        "AST",
        "TOV",
        "STL",
        "BLK",
        "BLKA",
        "PF",
        "PFD",
        "PTS",
        "PLUS_MINUS"   
    ]
    
    return game_log.loc[:, cols_order] 


@flow
def insert_game_logs_in_db():
    pass
