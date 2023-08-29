import objects
import requests
import sqlite3
import pandas as pd
import dateutil.parser as parser

##Helper stuff
def requestJsonBody(url):
    res = requests.get(url)
    if res.status_code!= 200:
        print("Bad res:"+str(res.status_code))
        return {}
    return res.json()

def parseDateToIso1801(date_str) -> str:
    return parser.parse(str(date_str)).isoformat()
#/#

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
            category=str(value["operating_area"])+ "->"+ str(value["perspective"]),
            source_id=value["id"]
        )
        for value in values
    ]

def fetch_kolada_datablocks(api_url) -> list:
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

def fetch_timeseries_for_kolada_kpi(api_url, data_id, source_id, geo_id) -> pd.DataFrame:
    cols = {"variable":[], "date":[], "value":[], "geo_id":[], "data_id":[]}
    url = '%s/data/kpi/%s/municipality/%s' % (api_url, source_id, geo_id)
    
    data = requestJsonBody(url)
    values = data["values"]
    for kpi_meta in values:
        datapoints = kpi_meta["values"][0]
        measure_val = datapoints["value"]
        if measure_val == None:
            continue
        cols["data_id"].append(data_id)
        cols["value"].append(measure_val)
        cols["variable"].append(datapoints["gender"])
        #clean date
        date = parseDateToIso1801(kpi_meta["period"])
        cols["date"].append(date)
        #clean stupid municipality code (seriously Kolada, why do you make me do this!???)
        geo_id = kpi_meta["municipality"]
        if len(geo_id) == 3:
            geo_id = "0" + str(geo_id)
        cols["geo_id"].append(geo_id)

    return pd.DataFrame(cols)

#TODO: Make async
def fetch_timeseries_for_kpi_tuples(kpi_tuples:dict,api_url:str) -> pd.DataFrame: 
    df_list = []
    communes = db_get_commune_ids()
    for data_id, source_id in kpi_tuples.items():
        for commune in communes:
            df = fetch_timeseries_for_kolada_kpi(api_url, data_id, source_id, commune)
            df_list.append(df)
    return pd.concat(df_list)

def db_get_commune_ids() -> list:
    cur = conn.cursor()
    cur.execute('SELECT geo_id FROM geo_unit WHERE type = "commune" LIMIT 20')
    rows = cur.fetchall()
    id_list = [row[0] for row in rows]
    return id_list

def db_get_kolada_datablock_source_ids() -> list:
    cur = conn.cursor()
    cur.execute('SELECT source_id FROM data_block where source = "Kolada" LIMIT 10')
    rows = cur.fetchall()
    id_list = [row[0] for row in rows]
    print(id_list[100])
    return id_list

def db_insert_timeseries(df:pd.DataFrame):
    df.to_sql(con=conn, name='timeseries', if_exists='append', index=False)
    conn.commit()
    conn.close()
    
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

#EXECUTION
api_url = "http://api.kolada.se/v2"
conn = sqlite3.connect('db/stat-db.db')

kpis = {
    883: "N02466",
    49: "N00100",
    5727: "U70451",
    2776: "N28818",
    1862: "N15533",
    2020: "N17020",
    3994: "N85527",
    1018: "N02993",
    4868: "U26473",
    1048: "N03038",
    3631: "N72025",
    4042: "U00308",
    3153: "N60098",
    3885: "N80713",
    5395: "U33904",
    5525: "U40402",
    1083: "N03074",
    1833: "N15504",
    712: "N02101",
}

#df = fetch_timeseries_for_kpi_tuples(kpis, api_url)
#db_insert_timeseries(df)
data = fetch_kolada_datablocks(api_url)
db_upsert_kolada_datablocks(data)