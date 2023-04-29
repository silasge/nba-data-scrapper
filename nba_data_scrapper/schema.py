from prefect import flow, task

from nba_data_scrapper.conn import connect_to_db


@task
def read_schema(sql):
    with open(sql) as f:
        sql = f.read()
    return sql


@flow
def create_schemas(db, schema_file):
    conn = connect_to_db(db=db)
    sql = read_schema(sql=schema_file)
    conn.execute(sql)
    return conn
