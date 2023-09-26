import sqlite3
import json
from models import models


class Writer:
    conn: sqlite3.Connection

    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.commit()

    def close(self):
        self.conn.close()

    def upsert_datablocks(self, datablocks: list[models.DataBlockBase]):
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
                        (block.title, block.type, block.source, block.tags, block.source_id, block.integration_id,
                         block.var_labels, block.geo_groups, block.description)
                        )
        self.conn.commit()

    def insert_timeseries(self, ts: models.Timeseries):
        ts.df.to_sql(con=self.conn, name='timeseries', if_exists='append', index=False)
        self.conn.commit()

    def update_meta(self, data_id: int, meta: dict):
        meta_json = json.dumps(meta)
        q = "UPDATE data_block SET meta = ? WHERE data_id = ?"
        cur = self.conn.cursor()
        cur.execute(q, (meta_json, data_id))
        self.conn.commit()

    def clear_timeseries_for_integration(self, integration_id: int):
        q = 'DELETE FROM timeseries WHERE data_id IN (SELECT data_id FROM data_block WHERE integration_id = (?))'
        cur = self.conn.cursor()
        cur.execute(q, (integration_id,))
        self.conn.commit()
