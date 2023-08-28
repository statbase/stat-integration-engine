import objects
import requests
import sqlite3
import pandas

def requestJsonBody(url):
    print(url)
    res = requests.get(url)
    if res.status_code!= 200:
        print("Bad res:"+res.status_code)
        return {}
    return res.json()

def datablocks_from_kolada_api(url) ->list:
    data = requestJsonBody(url)
    if data["count"] == 0:
        return None
    values = data["values"]
    return [
        objects.DataBlock(
            title=value["title"], 
            type="timeseries",
            source="Kolada",
            category=value["operating_area"],
            source_id=value["id"]
        )
        for value in values
    ]

def fetch_kolada_datablocks(api_url) -> list[str]:
    url = api_url + "/kpi"
    datablocks = datablocks_from_kolada_api(url)

    #Fetch rest of pages if any
    page = 2
    while True:
        paged_url = url + '?&page=%d' % page
        next_blocks = datablocks_from_kolada_api(paged_url)
        if next_blocks == None:
            break
        datablocks.extend(next_blocks)
        page +=1
    return datablocks

def fetch_timeseries_for_kolada_kpi(api_url, kpi_id, geo_id) -> pandas.DataFrame:
    cols = {"variable":[], "date":[], "value":[], "geo_id":[]}
    url = '%s/data/kpi/%s/municipality/%s' % (api_url, kpi_id, geo_id)
    
    data = requestJsonBody(url)
    values = data["values"]
    for kpi_meta in values:
        datapoints = kpi_meta["values"][0]
        measure_val = datapoints["value"]
        if measure_val == None:
            continue
        cols["value"].append(measure_val)
        cols["variable"].append(datapoints["gender"])
        cols["date"].append(kpi_meta["period"])
        cols["geo_id"].append(kpi_meta["municipality"])
    return pandas.DataFrame(cols)

def fetch_timeseries_for_kpi_list(kpi_list:list,api_url:str) -> pandas.DataFrame:
    communes = db_get_communes()

    for kpi_id in kpi_list:
        for commune in communes:
            print(fetch_timeseries_for_kolada_kpi(api_url, kpi_id, commune))
    pass


def db_get_communes() -> list:
    cur = conn.cursor()
    cur.execute('SELECT geo_id FROM geo_unit WHERE type = "commune"')
    rows = cur.fetchall()
    id_list = [row[0] for row in rows]
    return id_list

def db_upsert_kolada_datablocks(datablocks:list[objects.DataBlock]):
    cur = conn.cursor()
    for block in datablocks:
        cur.execute("""
        INSERT INTO data_block(title, type, source, category, source_id)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
        title = excluded.title,
        type = excluded.type,
        source = excluded.source,
        category = excluded.category
        """,
        (block.title, block.type, block.source, block.category, block.source_id)
        )
    conn.commit()
    conn.close()

#EXECUTION#
api_url = "http://api.kolada.se/v2"
conn = sqlite3.connect('db/stat-db.db')

#kolada_blocks = fetch_kolada_datablocks(api_url)
#db_upsert_kolada_datablocks(kolada_blocks)
#db_get_communes()

fetch_timeseries_for_kpi_list(['N00945'], api_url)
