import pandas

class DataBlock:
    def __init__(self, data: pandas.DataFrame, meta: dict, id: int):
        self.data = data
        self.meta = meta
        self.id = id
    def fetch(self): # Include logic as func param
        pass
    def validate(self):
        pass

class Entity:
    def __init__(self, name: str, geo_id: int, id: int):
        self.name = name
        self.geo_id = geo_id
        self.id = id
        
class Geo:
    def __init__(self, id: int, type: str):
        self.id = id
        self.type = type

class Category:
    def __init__(self, id: int, value: str, parent_id: int):
        self.id = id
        self.value = value
        self.parent_id = parent_id

class Tag:
    def __init__(self, id: int, value: str):
        self.id = id
        self.value = value