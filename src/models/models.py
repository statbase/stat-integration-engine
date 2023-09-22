import pandas as pd
from pydantic import BaseModel, Field

ts_cols = ["data_id", "value", "variable", "geo_id", "date"]
meta_keys = ['span', 'resolution', 'var_values']


def stringify_ts_columns():
    return ', '.join(ts_cols)


class SourceDataBlock(BaseModel):
    title: str = Field(..., min_length=1)
    type: str = Field(..., min_length=1)
    source: str = Field(..., min_length=1)
    source_id: str = Field(..., min_length=1)
    tags: str | list[str] = Field(..., min_length=1)
    integration_id: int = Field(..., ge=1)
    var_labels: str = Field(..., min_length=1)
    geo_groups: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)


class NormalisedDataBlock(SourceDataBlock):
    data_id: int = Field(ge=1)
    meta: dict = Field()


class Timeseries:
    df: pd.DataFrame

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.verify()

    def verify(self):
        if len(self.df.columns) != len(ts_cols):
            raise Exception(f"Invalid n. of columns, expected: {len(ts_cols)}, got:{len(self.df.columns)}")
        for col in ts_cols:
            if col not in self.df:
                raise Exception(f"Column {col} was not expected")
