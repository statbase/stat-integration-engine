import sqlite3
import objects.objects as objects

class Writer:
    conn: sqlite3.Connection
    def __init__(self, db_path:str):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.commit()

    def close(self):
        self.conn.close()
    
    def upsert_datablocks(self, datablocks:list[objects.SourceDataBlock]):
        cur = self.conn.cursor()
        for block in datablocks:
            cur.execute("""
            INSERT INTO data_block(title, type, source, tags, source_id, integration_id, var_labels, geo_groups, description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
            title = excluded.title,
            type = excluded.type,
            source = excluded.source,
            tags = excluded.tags,      
            var_labels = excluded.var_labels,
            geo_groups = excluded.geo_groups,
            description = excluded.description
            """,
            (block.title, block.type, block.source, block.tags, block.source_id, block.integration_id, block.var_labels, block.geo_groups, block.description)
            )
        self.conn.commit()

    def insert_timeseries(self, ts:objects.Timeseries):
        ts.df.to_sql(con=self.conn, name='timeseries', if_exists='append', index=False)
        self.conn.commit()
