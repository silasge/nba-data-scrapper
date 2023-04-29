import duckdb
import pandas as pd

def ingest_parquet_in_db(
    table: str,
    parquet: str,
    conn: duckdb.DuckDBPyConnection
):
    sql = f"""
    INSERT INTO {table}
    SELECT * FROM read_parquet('{parquet}');
    """
    conn.execute(sql)
    conn.commit()
    
    
def flag_home_away(x):
    try:
        if "vs." in x:
            return "Home"
        elif "@" in x:
            return "Away"
    except:
        return None