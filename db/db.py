import sqlite3
import objects
import pandas as pd

class db_conn:
    conn: sqlite3.Connection
    def __init__(self, db_path:str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row  

    def close(self):
        self.conn.close()

    def upsert_datablocks(self, datablocks:list[objects.SourceDataBlock]):
        cur = self.conn.cursor()
        for block in datablocks:
            cur.execute("""
            INSERT INTO data_block(title, type, source, category, source_id, integration_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
            title = excluded.title,
            type = excluded.type,
            source = excluded.source,
            category = excluded.category
            """,
            (block.title, block.type, block.source, block.category, block.source_id, block.integration_id)
            )
        self.conn.commit()

    def insert_timeseries(self, df:pd.DataFrame):
        df.to_sql(con=self.conn, name='timeseries', if_exists='append', index=False)
        self.conn.commit()

    def get_datablocks_by_source(self,source:str)-> list[objects.NormalisedDataBlock]: 
        out = []
        cur = self.conn.cursor()
        cur.execute('SELECT id, type, source, source_id, category, title, integration_id FROM data_block where source = (?) LIMIT 10', (source,))
        rows = cur.fetchall()
        for row in rows:
            out.append(objects.NormalisedDataBlock(
                data_id=row["id"],
                type=row["type"],
                source=row["source"],
                source_id=row["source_id"],
                category=row["category"],
                title=row["title"],
                integration_id=row["integration_id"]))
        return out
  
    def db_get_commune_ids(self) -> list:
        cur = self.conn.cursor()
        cur.execute('SELECT geo_id FROM geo_unit WHERE type = "commune" LIMIT 20')
        rows = cur.fetchall()
        id_list = [row[0] for row in rows]
        return id_list