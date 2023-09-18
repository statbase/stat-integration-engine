from fastapi import APIRouter, HTTPException
import objects.objects as objects
import db.read as dbread
import db.write as dbwrite
import json
import pandas as pd
import integrations.kolada as k
import integrations.integrations as i
import config.config as config


router = APIRouter()
kolada = k.KoladaIntegration()
geo_cache = i.GeoCache()
db_str = config.get("db_string")
dbreader = dbread.Reader
dbwriter = dbwrite.Writer

"""
HELPERS
"""
def db_read(func:callable, *args, **kwargs):
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

def db_write(func:callable, *args, **kwargs):
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

#Some would say this is hacky. I call it pragmatic! 
def parse_datablock_filter(filter:str) -> dict: 
    out = {}
    kwarg_list = filter.split(',')
    for kwarg in kwarg_list:
        key, val = kwarg.split('=',1)
        if key not in objects.SourceDataBlock.__annotations__: #Okay, this is pretty hacky... Well, come up with a better approach then!
            raise ValueError("key not in SourceDataBlock")
        if len(val) == 0:
            raise ValueError("len(val) is 0")
        out[key] = val
    return out

#TODO: make sure format is right, verify each geo_id
def parse_geo_ids(filter:str) -> list[str]: 
    if len(filter) > 50: #10 geo_ids + comma
        raise ValueError("to many characters in geo_ids filter (50 max)")
    geo_list = [id for id in filter.split(',')]
    for id in geo_list:
        if id not in geo_cache.get_geo_ids():
           raise ValueError("illegal geo_id provided")
    return geo_list
   
"""
HTTP HANDLERS 
"""
@router.get("/tags")
async def get_datablocks_for_tags():
    res = db_read(dbread.Reader.get_all_tags)
    return res
    
# If we don't have the timeseries, fetch it via integration and upload to db. 
# Yes, this can be GREATLY improved...
@router.get("/timeseries/{data_id}")
async def get_timeseries_by_id(data_id:int, geo_ids:str):
    id_list = parse_geo_ids(geo_ids)
    ts, missing_ids = db_read(dbreader.get_timeseries, data_id, id_list)
    if missing_ids:
        dblock_list = db_read(dbreader.get_datablocks_by_filters, data_id=data_id)
        fetched_ts = kolada.get_timeseries(dblock_list[0], missing_ids) #List should only contain 1 entry
        db_write(dbwriter.insert_timeseries, fetched_ts)
        ts.df = pd.concat([ts.df, fetched_ts.df])
    return json.loads(ts.df.to_json(orient="records"))

@router.get("/datablocks/tag/{tag}")
async def get_datablocks_by_tag(tag:str):
    res = db_read(dbreader.get_datablocks_by_filters,tags=tag)
    return res

@router.get("/datablocks/{id}")
async def get_datablocks_by_id(id:int):
    res = db_read(dbreader.get_datablocks_by_filters,data_id=id)
    return res

@router.get("/datablocks/search/{string}")
async def get_datablocks_by_search_string(string:str, filter:str | None=None):
    kwargs = None
    if filter:  
       kwargs = parse_datablock_filter(filter)
    res = db_read(dbreader.get_datablocks_by_search,string, kwargs)
    return res
