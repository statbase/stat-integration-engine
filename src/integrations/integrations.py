import src.objects.models as models
import requests
import re

import dateutil.parser as parser


class GeoCache:
    def __init__(self):
        self.ids = None

    def get_id_list(self):
        if self.ids is None:
            with open('integrations/files/geo_data.csv', 'r') as file:
                self.ids = [line.strip().split(';')[0] for line in file]
        return self.ids


# Helpers
def requestJsonBody(url):
    res = requests.get(url)
    if res.status_code != 200:
        raise ValueError("bad status code: " + res.status_code)
    return res.json()


def parseDate(date_str: str) -> str:
    # Only year -> day is 1st of january
    if len(str(date_str)) == 4:
        return str(date_str) + '-01-01'
    return parser.parse(str(date_str)).date()


def split_tags(tag_string: str) -> str:
    return re.sub(r',(?!\s)', ';', tag_string)


class BaseIntegration:
    base_url: str
    integration_id: int

    def __init__(self, url, id):
        self.base_url = url
        self.id = id

    def get_datablocks(self) -> list[models.SourceDataBlock]:
        """get datablocks from source"""
        pass

    def get_timeseries(self, dblock: models.NormalisedDataBlock) -> models.Timeseries:
        """get timeseries data from source for datablock"""
        pass
