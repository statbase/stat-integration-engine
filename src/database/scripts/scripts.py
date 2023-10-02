import os
import sqlite3
import pandas as pd
from config import config

cur_dir = os.path.dirname(os.path.realpath(__file__))


def get_geo_df() -> pd.DataFrame:
    return pd.read_csv(os.path.join(cur_dir, '../files', 'geo_data.csv'))


def create_db_schemas():
    conn = sqlite3.connect(config.get('DB_STRING'))
    with open(os.path.join(cur_dir, '../migrations', 'database_schema.sql'), 'r') as sql_file:
        sql = sql_file.read()
        cursor = conn.cursor()
        cursor.executescript(sql)
        conn.commit()
