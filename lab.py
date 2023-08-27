import objects
import requests
import sqlite3

conn = sqlite3.connect('db/scripts/stat-db.db')
api_url = "http://api.kolada.se/v2"

def datablocks_from_value_list(vl) ->list:
    return [
        objects.DataBlock(
            title=value["title"], 
            type="timeseries",
            source="Kolada",
            source_id=value["id"]
        )
        for value in vl
    ]

def list_kolada_kpis(conn: sqlite3.Connection):
    url = api_url + "/kpi"
    datablocks = []
    data = {}

    res = requests.get(url)
    if res.status_code!= 200:
        exit('Bad res: %s' % res.status_code)
    data = res.json()
    datablocks = datablocks_from_value_list(data["values"])

    #Loop through rest of pages if any ... notice some code repetition here...
    page = 2
    while data["count"] > 0:
        paged_url = url + '?&page=%d' % page
        data = requests.get(paged_url).json()
        if res.status_code!= 200:
            exit('Bad res: %s' % res.status_code)
        next_blocks = datablocks_from_value_list(data['values'])
        datablocks.extend(next_blocks)
        page +=1
    
    for block in datablocks:
        print ('%s : %s' % (block.source_id, block.title) )
    return datablocks

list_kolada_kpis(conn)


