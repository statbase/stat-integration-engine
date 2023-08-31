from dataclasses import dataclass
from pydantic import BaseModel, Field

class SourceDataBlock(BaseModel):
    title: str = Field(..., min_length=1)
    type:str = Field(..., min_length=1)
    source: str  = Field(..., min_length=1)
    source_id: str = Field(..., min_length=1)
    category: str = Field(..., min_length=1)
    integration_id: int = Field(..., ge=1)
    var_labels: str = Field(..., min_length=1)
    geo_groups:str = Field(..., min_length=1)

    def fetch(self): # Include logic as func param
        pass
    def validate(self):
        pass

class NormalisedDataBlock(SourceDataBlock):
    data_id:int = Field(ge=1)

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

class ts_row:
    def __init__(self, variable:str, date:str, value:str, geo_id:str, data_id:str):
        self.variable=variable
        self.date=date,
        self.value=value,
        self.geo_id=geo_id,
        self.data_id=data_id
    def to_dict(self):
        return  {"variable":self.variable, "date":self.date, "value":self.value, "geo_id":self.geo_id, "data_id":self.data_id}