import pandas
import models.models as models
import requests
import os
import dateutil.parser as parser

cur_dir = os.path.dirname(os.path.realpath(__file__))


class GeoCache:
    def __init__(self):
        self.ids = None

    def get_id_list(self):
        if self.ids is None:
            with open(os.path.join(cur_dir, 'files', 'geo_data.csv'), 'r') as file:
                self.ids = [line.strip().split(';')[0] for line in file]
        return self.ids


def request_json(url):
    res = requests.get(url)
    if res.status_code != 200:
        raise ValueError("bad status code: " + res.status_code)
    return res.json()


def parse_date(date_str: str) -> str:
    # Only year -> day is 1st of january
    if len(str(date_str)) == 4:
        return str(date_str) + '-01-01'
    return parser.parse(str(date_str)).date()


class BaseIntegration:
    base_url: str
    integration_id: int

    def get_datablocks(self) -> list[models.DataBlockBase]:
        """get datablocks from source"""
        pass

    def get_timeseries(self, dblock: models.DataBlock, geo_list: [str]) -> pandas.DataFrame:
        """get timeseries data from source for datablock"""
        pass
