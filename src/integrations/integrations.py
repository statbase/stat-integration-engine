import objects.objects as objects
import pandas as pd
import requests
import re

import dateutil.parser as parser

#Helpers
def requestJsonBody(url):
    res = requests.get(url)
    if res.status_code!= 200:
        raise ValueError("bad status code: " +res.status_code)
    return res.json()

def parseDate(date_str:str) -> str:
    #Only year -> day is 1st of january
    if len(str(date_str)) == 4:
        return str(date_str) + '-01-01'
    return parser.parse(str(date_str)).date()

def split_tags(tag_string:str) ->str:
    return re.sub(r',(?!\s)', ';', tag_string)

#temp: cap to 10 until we figure out to collect ts in parallel...
def get_geo_ids():
    with open('integrations/files/geo_data.csv', 'r') as file:
        ids = []
        for line in file:
            id, name = line.strip().split(';')
            ids.append(id)
        return ids[:10]

class BaseIntegration:
    base_url: str
    integration_id: int
    def __init__(self, url, id):
        self.base_url = url
        self.id = id
    def get_datablocks(self)->list[objects.SourceDataBlock]:
        """get datablocks from source"""
        pass
    def get_timeseries(self, dblocks:list[objects.NormalisedDataBlock])->objects.Timeseries:
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
    
    """
    It's messy, but give me a break! Normalising data 
    from a proprietary API does not have to be pretty! 
    Damnit, if you don't like it; come up with a fixing PR or shut up!
    """
    def get_timeseries(self, dblocks:list[objects.NormalisedDataBlock])->objects.Timeseries:
        cols = {col : [] for col in objects.ts_cols}
        id_list = get_geo_ids()
        for dblock in dblocks:
            for geo_id in id_list:
                url = '%s/data/kpi/%s/municipality/%s' % (self.base_url, dblock.source_id, geo_id)
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
                'description': value['description'],
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


