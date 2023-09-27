import sqlite3
import config.config as config
import os

cur_dir = os.path.dirname(os.path.realpath(__file__))


def run_schema_migration(conn: sqlite3.Connection):
    cursor = conn.cursor()
    with open(os.path.join(cur_dir, '../migrations/database_schema.sql'), 'r') as sql_file:
        sql = sql_file.read()
        cursor.executescript(sql)
        conn.commit()

    with open(os.path.join(cur_dir, 'scripts.py'), 'r') as script_file:
        code = script_file.read()
        exec(code)


conn = sqlite3.connect(config.get("db_string"))
run_schema_migration(conn)
