from dataclasses import dataclass

@dataclass
class SourceDataBlock:
    title: str
    type:str
    source: str
    source_id: str
    category: str
    integration_id: int
    def __init__(self, title: str, source: str, type: str, source_id: str, category: str, integration_id:int):
        self.title = title
        self.type = type
        self.source = source
        self.source_id = source_id
        self.category = category
        self.integration_id = integration_id
    def fetch(self): # Include logic as func param
        pass
    def validate(self):
        pass

@dataclass
class NormalisedDataBlock(SourceDataBlock):
    data_id:int
    def __init__(self, title: str, source: str, type: str, source_id: str, category: str, data_id:int, integration_id:int):
        self.title = title
        self.type = type
        self.source = source
        self.source_id = source_id
        self.category = category
        self.data_id = data_id
        self.integration_id = integration_id

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