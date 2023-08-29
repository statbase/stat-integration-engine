import objects
import pandas as pd
import requests
import sqlite3

import dateutil.parser as parser

#Helpers
def requestJsonBody(url):
    res = requests.get(url)
    if res.status_code!= 200:
        print("Bad res:"+str(res.status_code))
        return {}
    return res.json()

def parseDateToIso1801(date_str) -> str:
    return parser.parse(str(date_str)).isoformat()

#move to own class
conn = sqlite3.connect('db/stat-db.db')

def db_get_commune_ids() -> list:
    cur = conn.cursor()
    cur.execute('SELECT geo_id FROM geo_unit WHERE type = "commune" LIMIT 20')
    rows = cur.fetchall()
    id_list = [row[0] for row in rows]
    return id_list


class IntegrationInterface:
    base_url: str
    def __init__(self, url):
        self.base_url = url
    def get_datablocks(self, url: str)->objects.DataBlock:
        """get datablocks from source"""
        pass
    def get_timeseries(self, dblock:objects.DataBlock)->pd.DataFrame:
        """get timeseries data from source for datablock"""
        pass

class KoladaIntegration(IntegrationInterface):
    base_url: str

    def __init__(self, url):
        self.base_url = url

    def get_datablocks(self)->list[objects.DataBlock]:
        url = self.base_url + "/kpi"
        datablocks = self.datablocks_from_kolada_endpoint(url)
        #Fetch rest of pages if any
        page = 2
        while page < 10: #10 seems pretty high for Kolada
            paged_url = url + '?&page=%d' % page
            next_blocks = self.datablocks_from_kolada_endpoint(paged_url)
            if next_blocks == None:
                break
            datablocks.extend(next_blocks)
            page +=1
        return datablocks
    
    def get_timeseries(self, dblock:objects.DataBlock)->pd.DataFrame:
        cols = {"variable":[], "date":[], "value":[], "geo_id":[], "data_id":[]}
        communes = db_get_commune_ids()
        for id in communes:
            url = '%s/data/kpi/%s/municipality/%s' % (self.base_url, dblock.source_id, id)
            data = requestJsonBody(url)
            values = data["values"]
            for kpi_meta in values:
                datapoints = kpi_meta["values"][0]
                measure_val = datapoints["value"]
                if measure_val == None:
                    continue
                cols["data_id"].append(dblock.data_id)
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

    def datablocks_from_kolada_endpoint(url) ->list:
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

