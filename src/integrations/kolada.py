import re
import pandas as pd

from integrations import integrations as i
from models import models


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
        url = self.base_url + '/kpi'
        datablocks = self.datablocks_from_kolada_endpoint(url)
        # Fetch rest of pages if any
        page = 2
        while page < 10:
            paged_url = f'{url}?&page={page}'
            next_blocks = self.datablocks_from_kolada_endpoint(paged_url)
            if not next_blocks:
                break

            datablocks.extend(next_blocks)
            page += 1
        return datablocks

    """
    It's pretty messy, but works. Not prioritizing cleanup, since proprietary API:s,
    might break and need rewriting at any time.
    """

    def get_timeseries(self, dblock: models.DataBlock, geo_list: list[str]) -> pd.DataFrame:
        cols = {col: [] for col in models.ts_cols}
        for geo_id in geo_list:
            url = f'{self.base_url}/data/kpi/{dblock.source_id}/municipality/{geo_id}'
            try:
                data = i.request_json(url)
            except Exception as e:
                print(f'requesting json from API failed: {str(e)}')
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
                    # clean stupid municipality code
                    geo_id = year['municipality']
                    if len(geo_id) == 3:
                        geo_id = '0' + str(geo_id)
                    cols['geo_id'].append(geo_id)
        return pd.DataFrame(cols)

# TODO! TODO: Should maybe be simple injection in caller.
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
                'tags': split_tags(str(value['perspective'])) + ';'
                        + split_tags(str(value['operating_area'])),
                'source_id': value['id'],
                'integration_id': self.integration_id,
                'var_labels': 'Kön',
                'description': value['description'],
                'geo_groups': set_geo_group(value['municipality_type'])})
            for value in values]
