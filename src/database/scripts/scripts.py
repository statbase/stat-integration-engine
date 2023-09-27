import pandas as pd
import os

cur_dir = os.path.dirname(os.path.realpath(__file__))


def geo_df() -> pd.DataFrame:
    return pd.read_csv(os.path.join(cur_dir, '../files', 'geo_data.csv'))


def schema_sql() -> str:
    with open(os.path.join(cur_dir, '../migrations', 'database_schema.sql')) as sql_file:
        return sql_file.read()
