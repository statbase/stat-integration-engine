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

class Reader:
    conn: sqlite3.Connection
    def __init__(self, db_path:str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row  

    def close(self):
        self.conn.close()

    """
    TODO:
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
    
    def get_timeseries(self, data_id:int, geo_list:list[str]) -> tuple[objects.Timeseries, list[str]]:
        ts_columns = objects.stringify_ts_columns()
        placeholders = ','.join(['?'] * len(geo_list))
        if geo_list:
            q = f"SELECT {ts_columns} FROM timeseries WHERE data_id = ? AND geo_id IN ({placeholders})"
            params = [data_id] + geo_list
        else:
            q = f"SELECT {ts_columns} FROM timeseries WHERE data_id = ?"
            params = [data_id]
        
        df = pd.read_sql_query(q, self.conn, params=params)
        #TODO: figure out more efficient way to do this
        found_geo_ids = set(df['geo_id'].tolist())
        missing_geo_ids = [geo_id for geo_id in geo_list if geo_id not in found_geo_ids]

        return objects.Timeseries(df), missing_geo_ids
    
    def get_datablocks_by_filters(self, filters:dict):
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
        q +=   """
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