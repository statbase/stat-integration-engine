import src.integrations.integrations as i
import src.models.models as models
import pandas as pd
import re


def set_geo_group(label: str):
    if label == 'L':
        return 'R'
    if label == 'A':
        return 'A'
    return 'C'


def set_variable(var: str):
    if var == 'T':
        return 'Totalt'
    if var == 'M':
        return 'Män'
    if var == 'K':
        return 'Kvinnor'
    return var


def split_tags(tag_string: str) -> str:
    return re.sub(r',(?!\s)', ';', tag_string)


class KoladaIntegration(i.BaseIntegration):
    def __init__(self):
        self.base_url = 'http://api.kolada.se/v2'
        self.integration_id = 1

    def get_datablocks(self) -> list[models.SourceDataBlock]:
        url = self.base_url + "/kpi"
        datablocks = self.datablocks_from_kolada_endpoint(url)
        # Fetch rest of pages if any
        page = 2
        while page < 10:
            paged_url = url + '?&page=%d' % page
            next_blocks = self.datablocks_from_kolada_endpoint(paged_url)
            if not next_blocks:
                break
            datablocks.extend(next_blocks)
            page += 1
        return datablocks

    """
    It's messy, but give me a break! Normalising data 
    from a proprietary API does not have to be pretty! 
    Damnit, if you don't like it; come up with a fixing PR or shut up!
    """

    def get_timeseries(self, dblock: models.NormalisedDataBlock, geo_list: list[str]) -> models.Timeseries:
        cols = {col: [] for col in models.ts_cols}
        for geo_id in geo_list:
            url = '%s/data/kpi/%s/municipality/%s' % (self.base_url, dblock.source_id, geo_id)
            try:
                data = i.request_json(url)
            except Exception as e:
                print(f'requesting json from API failed: {e}')
                continue
            for year in data['values']:
                datapoints = year['values']
                for datapoint in datapoints:
                    measure_val = datapoint['value']
                    if measure_val is None:
                        continue
                    cols['data_id'].append(dblock.data_id)
                    cols['value'].append(measure_val)
                    cols['variable'].append(set_variable(datapoint['gender']))
                    date = i.parse_date(year['period'])
                    cols['date'].append(date)
                    # clean stupid municipality code (seriously Kolada, why do you make me do this!???)
                    geo_id = year['municipality']
                    if len(geo_id) == 3:
                        geo_id = '0' + str(geo_id)
                    cols['geo_id'].append(geo_id)
        df = pd.DataFrame(cols)
        return models.Timeseries(df)

    def datablocks_from_kolada_endpoint(self, url) -> list:
        data = i.request_json(url)
        if data['count'] == 0:
            return []
        values = data['values']
        return [
            models.SourceDataBlock(**{
                'title': value['title'],
                'type': 'timeseries',
                'source': 'Kolada',
                'tags': split_tags(str(value['perspective'])) + ';' + split_tags(str(value['operating_area'])),
                'source_id': value['id'],
                'integration_id': self.integration_id,
                'var_labels': 'Kön',
                'description': value['description'],
                'geo_groups': set_geo_group(value['municipality_type'])})
            for value in values]

