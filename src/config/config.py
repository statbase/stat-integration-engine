import json

with open("config/config.json", "r") as config_file:
    config = json.load(config_file)

def get(str:str)->str: 
    return config.get(str)