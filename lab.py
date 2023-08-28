import objects
import requests
import sqlite3

def datablocks_from_kolada_api(url) ->list:
    res = requests.get(url)
    if res.status_code!= 200:
        exit('Bad res: %s' % res.status_code)
    data = res.json()
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

def fetch_kolada_datablocks(api_url) -> list[objects.DataBlock]:
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

#Maybe a function to run from the db object itself?
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

kolada_blocks = fetch_kolada_datablocks(api_url)
db_upsert_kolada_datablocks(kolada_blocks)
