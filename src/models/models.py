from pydantic import BaseModel, Field

ts_cols = ["data_id", "value", "variable", "geo_id", "date"]
meta_keys = ['span', 'resolution', 'var_values']


class DataBlockBase(BaseModel):
    title: str = Field(..., min_length=1)
    type: str = Field(..., min_length=1)
    source: str = Field(..., min_length=1)
    source_id: str = Field(..., min_length=1)
    tags: str | list[str] = Field(..., min_length=1)
    integration_id: int = Field(..., ge=1)
    var_labels: str = Field(..., min_length=1)
    geo_groups: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)


class DataBlock(DataBlockBase):
    data_id: int = Field(ge=1)
    meta: dict = Field()

    class Config:
        from_attributes = True
