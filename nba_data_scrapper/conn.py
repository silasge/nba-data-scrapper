import duckdb
from prefect import task


@task
def connect_to_db(db):
    return duckdb.connect(database=db)
