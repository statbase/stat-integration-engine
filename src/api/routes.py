import json
import pandas as pd

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session

from models import models
from integrations import kolada as k
from integrations.integrations import GeoCache
from database import database, crud


router = APIRouter()
kolada = k.KoladaIntegration()
geo_cache = GeoCache()


def get_session():
    session = database.Session()
    try:
        yield session
    finally:
        session.close()


# Some would say this is hacky. I call it pragmatic!
def parse_datablock_filter(filter: str) -> dict:
    if filter is None:
        return {}
    out = {}
    kwarg_list = filter.split(',')
    for kwarg in kwarg_list:
        key, val = kwarg.split('=', 1)
        if key not in models.DataBlockBase.__annotations__:  # Okay, this is pretty hacky...
            raise ValueError(f"invalid filter key: '{key}'")
        if len(val) == 0:
            raise ValueError("length of filter value is 0")
        out[key] = val
    return out


def parse_tags(tag_string: str) -> list[str]:
    if tag_string is None:
        return []
    return tag_string.split(',')


def parse_geo_ids(id_list: str) -> list[str]:
    if len(id_list) > 50:  # 10 geo_ids + comma
        raise ValueError("to many characters in geo_ids filter (>50)")
    geo_list = id_list.split(',')
    for id in geo_list:
        if id not in geo_cache.get_id_list():
            raise ValueError(f"illegal geo_id provided: {id}")
    return geo_list


"""ROUTES"""


@router.get("/tags")
async def get_all_tags(filter: str = None, session: Session = Depends(get_session)):
    tag_list = parse_tags(filter)
    res = crud.get_tags(session, tag_list)
    return res


# If we don't have the timeseries, fetch it via integration and upload to db. 
# Yes, this can be improved...
@router.get("/timeseries/{data_id}")
async def get_timeseries_by_id(data_id: int, geo_ids: str, db: Session = Depends(get_session)):
    try:
        id_list = parse_geo_ids(geo_ids)
    except ValueError as e:
        return HTTPException(422, str(e))
    ts, missing_ids = crud.get_timeseries(db, data_id, id_list)
    if missing_ids:
        dblock_list = crud.get_datablocks(db, data_id=data_id)
        ts_fetched = kolada.get_timeseries(dblock_list[0], missing_ids)
        crud.insert_timeseries(db, ts_fetched)
        ts = pd.concat([ts, ts_fetched])
    ts.drop(['ts_id', 'data_id'], axis=1, inplace=True)
    return json.loads(ts.to_json(orient="records"))


@router.get("/datablocks/{id}")
async def get_datablocks_by_id(id: int, db: Session = Depends(get_session)):
    return crud.get_datablocks(db, data_id=id)[0]


@router.get("/datablocks")
async def get_datablocks(search_term: str = '', filter: str = None, tags: str = None,
                         session: Session = Depends(get_session)):
    tag_list = parse_tags(tags)
    try:
        filter_args = parse_datablock_filter(filter)
    except ValueError as e:
        return HTTPException(422, detail=str(e))
    return crud.get_datablocks(session, search_term, tag_list, **filter_args)


@router.get('/geo')
async def get_geo_list(db: Session = Depends(get_session)):
    return crud.get_geo_list(db)
