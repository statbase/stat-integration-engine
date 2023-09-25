from fastapi import APIRouter, HTTPException
import src.models.models as models
import src.db.read as dbread
import src.db.write as dbwrite
import json
import pandas as pd
import src.integrations.kolada as k
import src.integrations.integrations as i
import src.config.config as config

router = APIRouter()
kolada = k.KoladaIntegration()
geo_cache = i.GeoCache()
db_str = config.get("db_string")
dbreader = dbread.Reader
dbwriter = dbwrite.Writer


def db_read(func: callable, *args, **kwargs):
    conn = dbread.Reader(db_str)
    try:
        if args and kwargs:
            res = func(conn, *args, **kwargs)
        elif args:
            res = func(conn, *args)
        elif kwargs:
            res = func(conn, **kwargs)
        else:
            res = func(conn)
    finally:
        conn.close()
    return res


def db_write(func: callable, *args, **kwargs):
    conn = dbwrite.Writer(db_str)
    try:
        if args and kwargs:
            res = func(conn, *args, **kwargs)
        elif args:
            res = func(conn, *args)
        elif kwargs:
            res = func(conn, **kwargs)
        else:
            res = func(conn)
    finally:
        conn.close()
    return res


# Some would say this is hacky. I call it pragmatic!
def parse_datablock_filter(filter: str) -> dict:
    out = {}
    kwarg_list = filter.split(',')
    for kwarg in kwarg_list:
        key, val = kwarg.split('=', 1)
        if key not in models.SourceDataBlock.__annotations__:  # Okay, this is pretty hacky...
            raise ValueError(f"invalid filter key: '{key}'")
        if len(val) == 0:
            raise ValueError("length of filter value is 0")
        out[key] = val
    return out


def parse_geo_ids(id_list: str) -> list[str]:
    if len(id_list) > 50:  # 10 geo_ids + comma
        raise ValueError("to many characters in geo_ids filter (>50)")
    geo_list = [id for id in id_list.split(',')]
    for id in geo_list:
        if id not in geo_cache.get_id_list():
            raise ValueError("illegal geo_id provided")
    return geo_list


"""ROUTES"""


@router.get("/tags")
async def get_datablocks_for_tags():
    res = db_read(dbread.Reader.get_all_tags)
    return res


# If we don't have the timeseries, fetch it via integration and upload to db. 
# Yes, this can be improved...
@router.get("/timeseries/{data_id}")
async def get_timeseries_by_id(data_id: int, geo_ids: str):
    try:
        id_list = parse_geo_ids(geo_ids)
    except ValueError as e:
        return HTTPException(422, e)

    ts, missing_ids = db_read(dbreader.get_timeseries, data_id, id_list)
    if missing_ids:
        dblock_list = db_read(dbreader.get_datablocks_by_filters, data_id=data_id)
        fetched_ts = kolada.get_timeseries(dblock_list[0], missing_ids)  # List should only contain 1 entry
        db_write(dbwriter.insert_timeseries, fetched_ts)
        ts.df = pd.concat([ts.df, fetched_ts.df])
    return json.loads(ts.df.to_json(orient="records"))


@router.get("/datablocks/tag/{tag}")
async def get_datablocks_by_tag(tag: str):
    res = db_read(dbreader.get_datablocks_by_filters, tags=tag)
    return res


@router.get("/datablocks/{id}")
async def get_datablocks_by_id(id: int):
    res = db_read(dbreader.get_datablocks_by_filters, data_id=id)
    return res


@router.get("/datablocks/search/{string}")
async def get_datablocks_by_search_string(string: str, filter: str | None = None):
    if filter:
        try:
            kwargs = parse_datablock_filter(filter)
        except ValueError as e:
            return HTTPException(422, detail=str(e))
        res = db_read(dbreader.get_datablocks_by_search, string, **kwargs)
    else:
        res = db_read(dbreader.get_datablocks_by_search, string)
    return res


@router.get('/geo')
async def get_geo_list():
    return db_read(dbreader.get_geo_list)
