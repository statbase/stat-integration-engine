import sqlite3
import objects.objects as objects
import pandas as pd


#Helpers, move to separate classes maybe?
def dblock_from_rows(rows: list[sqlite3.Row]):
    return [
    objects.NormalisedDataBlock(**{
        "data_id": row["data_id"],
        "type": row["type"],
        "source": row["source"],
        "source_id": row["source_id"],
        "tags": row["tags"],
        "title": row["title"],
        "description": row["description"],
        "integration_id": row["integration_id"],
        "var_labels": row["var_labels"],
        "geo_groups": row["geo_groups"]})
        for row in rows]

def apply_filters(q:str, **filters):
    for k,v in filters.items():
        operator = '='
        if k == 'tags':
            operator = "LIKE"
            v = '%' + v + '%'
        q += f" {k} {operator} '{v}' AND"
    q = q[:-4]
    return q 

class db_conn:
    conn: sqlite3.Connection
    def __init__(self, db_path:str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row  
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.commit()

    def close(self):
        self.conn.close()

    """
    This is more expensive than it should be. 
    Consider either storing the tag + tag-datablock relations in separate tables instead
    Alternatively, load & cache this bad boy at startup. It ain't expected to change much during runtime, anyhow.
    """
    def get_all_tags(self) -> dict[str:str]:
        tag_counts = {}
        cur = self.conn.cursor()
        cur.execute('SELECT distinct tags from data_block') 
        self.conn.commit()
        rows = cur.fetchall()
        for row in rows:
            tags = str(row['tags']).split(';')
            for tag in tags:
                if tag in tag_counts.keys():
                    tag_counts[tag] +=1
                else:
                   tag_counts[tag] = 1
        return dict(sorted(tag_counts.items(), key=lambda count: count[1], reverse=True))
    

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

    def get_timeseries_by_id(self, data_id:int) -> objects.Timeseries:
        q = "SELECT %s FROM timeseries WHERE data_id = %i" % (objects.stringify_ts_columns(), data_id)
        df = pd.read_sql_query(q, self.conn)
        return objects.Timeseries(df)
    
    def get_datablocks_by_filters(self, **filters):
        cur = self.conn.cursor()
        if not filters:
            return []
        q = 'SELECT * FROM data_block WHERE '
        q = apply_filters(q, **filters)
        cur.execute(q)
        rows = cur.fetchall()
        return dblock_from_rows(rows)
    
    #Primitive as hell but works suprisingly well
    def get_datablocks_by_search(self,term:str, filters:dict)-> list[objects.NormalisedDataBlock]: 
        term = '%'+term+'%'
        q = "SELECT * FROM data_block WHERE (title LIKE (?) OR tags LIKE (?))"
        if filters:
            q += " AND "
            q = apply_filters(q, **filters)
        q = q + """
                ORDER BY 
                    CASE 
                        WHEN title LIKE (?) THEN 1
                        ELSE 2
                    END
                LIMIT 100
                """
        cur = self.conn.cursor()
        cur.execute(q, (term,term,term))
        rows = cur.fetchall()
        return dblock_from_rows(rows)
    
    #Rethink this, maybe... Should probably be stored in csv file and loaded into memory at startup
    def db_get_commune_ids(self) -> list:
        cur = self.conn.cursor()
        cur.execute('SELECT geo_id FROM geo_unit WHERE type = "commune" ORDER BY RANDOM() LIMIT 10')
        rows = cur.fetchall()
        id_list = [row[0] for row in rows]
        return id_list
    