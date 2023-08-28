import pandas
from dataclasses import dataclass

@dataclass
class DataBlock:
    title: str
    type:str
    source: str
    source_id: str
    category: str
    def __init__(self, title: str, source: str, type: str, source_id: str, category: str):
        self.title = title
        self.type = type
        self.source = source
        self.source_id = source_id
        self.category = category
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