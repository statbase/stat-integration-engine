import sqlite3
import objects.objects as objects
import pandas as pd

class db_conn:
    conn: sqlite3.Connection
    def __init__(self, db_path:str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row  
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.commit()

    def close(self):
        self.conn.close()

    def delete_datablocks_for_integration(self, integration_id:int):
        cur = self.conn.cursor()
        cur.execute("""
        DELETE FROM data_block
        WHERE integration_id = (?)
        """, (integration_id,))
        self.conn.commit()

    def upsert_datablocks(self, datablocks:list[objects.SourceDataBlock]):
        cur = self.conn.cursor()
        for block in datablocks:
            cur.execute("""
            INSERT INTO data_block(title, type, source, tags, source_id, integration_id, var_labels, geo_groups)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
            title = excluded.title,
            type = excluded.type,
            source = excluded.source,
            tags = excluded.tags,      
            var_labels = excluded.var_labels,
            geo_groups = excluded.geo_groups
            """,
            (block.title, block.type, block.source, block.tags, block.source_id, block.integration_id, block.var_labels, block.geo_groups)
            )
        self.conn.commit()

    #TODO: Receiver should be timeseries object. Transform Timeseries -> Dataframe in class method.
    def insert_timeseries(self, df:pd.DataFrame):
        df.to_sql(con=self.conn, name='timeseries', if_exists='append', index=False)
        self.conn.commit()

    def get_timeseries_by_id(self, data_id:int) -> pd.DataFrame:
        q = "SELECT * FROM timeseries WHERE data_id = %i" % data_id
        return pd.read_sql_query(q, self.conn)


       
    def get_datablocks_by_field(self,name:str,value)-> list[objects.NormalisedDataBlock]: 
        cur = self.conn.cursor()
        cur.execute("""
        SELECT data_id, type, source, source_id, tags, title, integration_id, var_labels, geo_groups
        FROM data_block WHERE (%s) = (?) AND geo_groups='C'  ORDER BY RANDOM() LIMIT 10
        """ % name, (value,))
        rows = cur.fetchall()
    
        return [
            objects.NormalisedDataBlock(**{
                "data_id": row["data_id"],
                "type": row["type"],
                "source": row["source"],
                "source_id": row["source_id"],
                "tags": row["tags"],
                "title": row["title"],
                "integration_id": row["integration_id"],
                "var_labels": row["var_labels"],
                "geo_groups": row["geo_groups"]})
                for row in rows]
    
    #Rethink this, maybe... Should probably be stored in csv file and loaded into memory at startup
    def db_get_commune_ids(self) -> list:
        cur = self.conn.cursor()
        cur.execute('SELECT geo_id FROM geo_unit WHERE type = "commune" ORDER BY RANDOM() LIMIT 10')
        rows = cur.fetchall()
        id_list = [row[0] for row in rows]
        return id_list
    

