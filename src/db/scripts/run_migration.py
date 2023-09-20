import sqlite3


def run_schema_migration(conn: sqlite3.Connection):
    cursor = conn.cursor()
    with open('db/migrations/database_schema.sql', 'r') as file:
        sql = file.read()
        cursor.executescript(sql)
        conn.commit()

    with open('db/scripts/geo_units.py', 'r') as script_file:
        code = script_file.read()
        exec(code)
