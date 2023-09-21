import sqlite3
import src.models.models as models
import pandas as pd
import json


def dblock_from_row_list(row_list: list) -> list[models.NormalisedDataBlock]:
    return [
        models.NormalisedDataBlock(**{
            "data_id": row["data_id"],
            "type": row["type"],
            "source": row["source"],
            "source_id": row["source_id"],
            "tags": str(row["tags"]).split(';'),
            "title": row["title"],
            "description": row["description"],
            "integration_id": row["integration_id"],
            "var_labels": row["var_labels"],
            "geo_groups": row["geo_groups"],
            "meta": json.loads((row['meta']))})
        for row in row_list]


def apply_filters(q: str, **filters):
    for k, v in filters.items():
        operator = '='
        if k == 'tags':
            operator = "LIKE"
            v = '%' + v + '%'
        q += f" {k} {operator} '{v}' AND"
    q = q[:-4]
    return q


class Reader:
    conn: sqlite3.Connection

    def __init__(self, db_path: str):
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
        cur.execute('SELECT tags from data_block')
        self.conn.commit()
        row_list = cur.fetchall()
        for row in row_list:
            tags = str(row['tags']).split(';')
            for tag in tags:
                if tag in tag_counts.keys():
                    tag_counts[tag] += 1
                else:
                    tag_counts[tag] = 1
        return dict(sorted(tag_counts.items(), key=lambda count: count[1], reverse=True))

    def get_timeseries(self, data_id: int, geo_list: list[str]) -> tuple[models.Timeseries, list[str]]:
        ts_columns = models.stringify_ts_columns()
        placeholders = ','.join(['?'] * len(geo_list))
        if geo_list:
            q = f"SELECT {ts_columns} FROM timeseries WHERE data_id = ? AND geo_id IN ({placeholders})"
            params = [data_id] + geo_list
        else:
            q = f"SELECT {ts_columns} FROM timeseries WHERE data_id = ?"
            params = [data_id]

        df = pd.read_sql_query(q, self.conn, params=params)
        # TODO: figure out more efficient way to do this
        found_geo_ids = set(df['geo_id'].tolist())
        missing_geo_ids = [geo_id for geo_id in geo_list if geo_id not in found_geo_ids]

        return models.Timeseries(df), missing_geo_ids

    def get_datablocks_by_filters(self, **filters):
        cur = self.conn.cursor()
        if not filters:
            return []
        q = 'SELECT * FROM data_block WHERE '
        q = apply_filters(q, **filters)
        cur.execute(q)
        row_list = cur.fetchall()
        return dblock_from_row_list(row_list)

    # Works assuming we have complete timeseries for at least 1 geo_id
    def calculate_meta(self, data_id: int) -> dict:
        def calculate_span(row_list: list) -> str:
            return row_list[0]['date'] + '-' + row_list[-1]['date']

        def calculate_resolution(row_list: list) -> str:
            return 'year'

        def calculate_var_values(row_list: list) -> list[str]:
            return [row['variable'] for row in row_list]

        meta = {}
        cur = self.conn.cursor()
        date_q = 'SELECT DISTINCT date FROM timeseries WHERE data_id = (?) ORDER BY date'
        var_q = 'SELECT DISTINCT variable FROM timeseries WHERE data_id = (?)'

        cur.execute(date_q, (data_id,))
        row_list = cur.fetchall()
        if not row_list:
            raise FileNotFoundError('no timeseries found')
        meta['span'] = calculate_span(row_list)
        meta['resolution'] = calculate_resolution(row_list)

        cur.execute(var_q, (data_id,))
        row_list = cur.fetchall()
        meta['var_values'] = calculate_var_values(row_list)

        return meta

    # Primitive as hell but works surprisingly well
    def get_datablocks_by_search(self, term: str, **filters) -> list[models.NormalisedDataBlock]:
        term = '%' + term + '%'
        q = "SELECT * FROM data_block WHERE (title LIKE (?) OR tags LIKE (?))"
        if filters:
            q += " AND "
            q = apply_filters(q, **filters)
        q += """
                ORDER BY 
                    CASE 
                        WHEN title LIKE (?) THEN 1
                        ELSE 2
                    END
                LIMIT 100
                """
        cur = self.conn.cursor()
        cur.execute(q, (term, term, term))
        row_list = cur.fetchall()
        return dblock_from_row_list(row_list)
