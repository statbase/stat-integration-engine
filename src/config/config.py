import json
import os

cur_dir = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(cur_dir, 'config.json'), "r") as config_file:
    config = json.load(config_file)


def get(str: str) -> str:
    return config.get(str)
