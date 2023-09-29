# coding: utf-8
from sqlalchemy import Column, ForeignKey, Index, Integer, Numeric, Text, UniqueConstraint, text
from sqlalchemy.sql.sqltypes import JSON
from sqlalchemy.orm import relationship
from .database import declarative_base as base

Base = base()
metadata = Base.metadata


class DataBlock(Base):
    __tablename__ = 'data_block'

    data_id = Column(Integer, primary_key=True)
    type = Column(Text, nullable=False)
    meta = Column(JSON, nullable=False, server_default=text("'{}'"))
    source = Column(Text, nullable=False)
    source_id = Column(Text, unique=True)
    tags = Column(Text)
    geo_groups = Column(Text)
    title = Column(Text)
    integration_id = Column(Integer)
    var_labels = Column(Text)
    description = Column(Text)


class GeoUnit(Base):
    __tablename__ = 'geo_unit'

    type = Column(Text, nullable=False)
    geo_id = Column(Text, primary_key=True)
    name = Column(Text)


class Timeseries(Base):
    __tablename__ = 'timeseries'
    __table_args__ = (
        UniqueConstraint('date', 'variable', 'geo_id', 'data_id'),
        Index('idx_timeseries_data_id_geo_id', 'data_id', 'geo_id')
    )

    ts_id = Column(Integer, primary_key=True)
    data_id = Column(ForeignKey('data_block.data_id', ondelete='CASCADE'), nullable=False, index=True)
    value = Column(Numeric, nullable=False)
    date = Column(Text, nullable=False)
    variable = Column(Text, nullable=False)
    geo_id = Column(ForeignKey('geo_unit.geo_id'), nullable=False)

    data = relationship('DataBlock')
    geo = relationship('GeoUnit')
