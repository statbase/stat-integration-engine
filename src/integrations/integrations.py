import objects.objects as objects
import pandas as pd
import requests
import db.db as db
import re

import dateutil.parser as parser

#Helpers
def requestJsonBody(url):
    res = requests.get(url)
    if res.status_code!= 200:
        print('Bad res:'+str(res.status_code))
        return {}
    return res.json()

def parseDate(date_str:str) -> str:
    #Only year -> day is 1st of january
    if len(str(date_str)) == 4:
        return str(date_str) + '-01-01'
    else:
        return parser.parse(str(date_str)).date()

def split_tags(tag_string:str) ->str:
    return re.sub(r',(?!\s)', ';', tag_string)

#temp: cap to 10 until we figure out to collect ts in parallel...
def get_commune_ids():
    with open('integrations/files/geo_data.csv', 'r') as file:
        ids = []
        for line in file:
            key, value = line.strip().split(';')
            if " län" in value:
               continue
            ids.append(key)
        return ids[:10]

class BaseIntegration:
    base_url: str
    integration_id: int
    def __init__(self, url, id):
        self.base_url = url
        self.id = id
    def get_datablocks(self, url: str)->objects.SourceDataBlock:
        """get datablocks from source"""
        pass
    def get_timeseries(self, dblock:objects.NormalisedDataBlock)->objects.Timeseries:
        """get timeseries data from source for datablock"""
        pass

class KoladaIntegration(BaseIntegration):
    def __init__(self):
        self.base_url = 'http://api.kolada.se/v2'
        self.integration_id = 1

    def get_datablocks(self)->list[objects.SourceDataBlock]:
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
    
    def get_timeseries(self, dblocks:list[objects.NormalisedDataBlock])->objects.Timeseries:
        cols = {col : [] for col in objects.ts_cols}
        communes = get_commune_ids()
        for dblock in dblocks:
            for commune_id in communes:
                url = '%s/data/kpi/%s/municipality/%s' % (self.base_url, dblock.source_id, commune_id)
                data = requestJsonBody(url)
                for year in data['values']:
                    datapoints = year['values']
                    for datapoint in datapoints:
                        measure_val = datapoint['value']
                        if measure_val == None:
                            continue
                        cols['data_id'].append(dblock.data_id)
                        cols['value'].append(measure_val)
                        cols['variable'].append(self.set_variable(datapoint['gender']))
                        date = parseDate(year['period'])
                        cols['date'].append(date)
                        #clean stupid municipality code (seriously Kolada, why do you make me do this!???)
                        geo_id = year['municipality']
                        if len(geo_id) == 3:
                            geo_id = '0' + str(geo_id)
                        cols['geo_id'].append(geo_id)
        df = pd.DataFrame(cols)
        return objects.Timeseries(df)

    def datablocks_from_kolada_endpoint(self, url) ->list:
        data = requestJsonBody(url)
        if data['count'] == 0:
            return None
        values = data['values']
        return [
            objects.SourceDataBlock(**{
                'title': value['title'],
                'type': 'timeseries',
                'source': 'Kolada',
                'tags': split_tags(str(value['perspective'])) + ';' + split_tags(str(value['operating_area'])),
                'source_id': value['id'],
                'integration_id': self.integration_id,
                'var_labels': 'Kön',
                'geo_groups': self.set_geo_group(value['municipality_type'])})
            for value in values]
    
    def set_geo_group(self, label:str):
        if label== 'L':
            return 'R'
        if label== 'A':
            return 'A'
        return 'C'
    
    def set_variable(self, var:str):
        if var== 'T':
            return 'Totalt'
        if var== 'M':
            return 'Män'
        if var== 'K':
            return 'Kvinnor'
        return var


