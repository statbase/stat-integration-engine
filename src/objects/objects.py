from dataclasses import dataclass
import pandas as pd
from pydantic import BaseModel, Field


ts_cols=["data_id", "value", "variable", "geo_id", "date"]

def stringify_ts_columns():
    return ', '.join(ts_cols)

class SourceDataBlock(BaseModel):
    title: str = Field(..., min_length=1)
    type:str = Field(..., min_length=1)
    source: str  = Field(..., min_length=1)
    source_id: str = Field(..., min_length=1)
    tags: str = Field(...,)
    integration_id: int = Field(..., ge=1)
    var_labels: str = Field(..., min_length=1)
    geo_groups:str = Field(..., min_length=1)
    description:str = Field(..., )

class NormalisedDataBlock(SourceDataBlock):
    data_id:int = Field(ge=1)

class Entity:
    def __init__(self, name: str, geo_id: int, id: int):
        self.name = name
        self.geo_id = geo_id
        self.id = id

class Timeseries:
    df:pd.DataFrame

    def __init__(self, df:pd.DataFrame):
        self.df=df
        self.verify()
        
    def verify(self):
        if len(self.df.columns) != len(ts_cols):
            raise Exception("Invalid n. of columns, expected: %i, got:%i" % (len(ts_cols), len(self.df.columns)))
        for col in ts_cols:
            if col not in self.df:
                raise Exception("Column %s was not expected" % col)
            