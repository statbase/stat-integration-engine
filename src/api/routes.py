from fastapi import APIRouter
import objects.objects as objects
import db.db as db
import json
import integrations.integrations as integrations

router = APIRouter()
kolada = integrations.KoladaIntegration()

"""
HELPERS
"""
def db_func(func:callable, *args, **kwargs):
    conn = db.db_conn("db/stat-db.db")
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
def parse_filter(filter:str) -> dict: 
    out = {}
    kwarg_list = filter.split(',')
    for kwarg in kwarg_list:
        key, val = kwarg.split('=')[0],kwarg.split('=')[1]
        if key not in objects.SourceDataBlock.__annotations__: #Okay, this is pretty hacky... Well, come up with a better approach then!
            raise ValueError("key not in SourceDataBlock")
        if len(val) == 0:
            raise ValueError("len(val) is 0")
        out[key] = val
    return out

"""
HTTP HANDLERS 
"""
@router.get("/tags")
async def get_datablocks_for_tags():
    res = db_func(db.db_conn.get_all_tags)
    return res
    
# If we don't have the timeseries, fetch it via integration and upload to db. 
# Yes, this can be GREATLY improved...
@router.get("/timeseries/{data_id}")
async def get_timeseries_by_id(data_id:int):
    ts = db_func(db.db_conn.get_timeseries_by_id, data_id)
    if len(ts.df)==0:
        dblock = db_func(db.db_conn.get_datablocks_by_filters, data_id=data_id)
        ts = kolada.get_timeseries(dblock)
        db_func(db.db_conn.insert_timeseries, ts)
    return json.loads(ts.df.to_json(orient="records"))

@router.get("/datablocks/tag/{tag}")
async def get_datablocks_by_tag(tag:str):
    res = db_func(db.db_conn.get_datablocks_by_filters,tags=tag)
    return res

@router.get("/datablocks/{id}")
async def get_datablocks_by_id(id:int):
    res = db_func(db.db_conn.get_datablocks_by_filters,data_id=id)
    return res

@router.get("/datablocks/search/{string}")
async def get_datablocks_by_search_string(string:str, filter:str | None=None):
    kwargs = None
    if filter is not None:  
       kwargs = parse_filter(filter)
    res = db_func(db.db_conn.get_datablocks_by_search,string, kwargs)
    return res
