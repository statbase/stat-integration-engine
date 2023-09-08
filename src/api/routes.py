from fastapi import APIRouter
import objects.objects as objects
import db.db as db
import json
import integrations.integrations as integrations

router = APIRouter()

kolada = integrations.KoladaIntegration()

def db_func(select_func:callable, *args):
    conn = db.db_conn("db/stat-db.db")
    if args:
        res = select_func(conn, *args)
    else:
        res = select_func(conn)
    conn.close()
    return res

@router.get("/tags")
def get_datablocks_for_tags():
    res = db_func(db.db_conn.get_all_tags)
    return res
    
# If we don't have the timeseries, fetch it via integration and upload to db. 
# Yes, this can be GREATLY improved...
@router.get("/timeseries/{data_id}")
def get_timeseries_by_id(data_id:int):
    ts = db_func(db.db_conn.get_timeseries_by_id, data_id)
    if len(ts.df)==0:
        dblock = db_func(db.db_conn.get_datablocks_by_field, "data_id", data_id, "=")
        ts = kolada.get_timeseries(dblock)
        db_func(db.db_conn.insert_timeseries, ts)

    return json.loads(ts.df.to_json(orient="records"))

@router.get("/datablocks/tag/{tag}")
def get_datablocks_by_tag(tag:str):
    res = db_func(db.db_conn.get_datablocks_by_field,"tags", "%"+tag+"%" ,"LIKE")
    return res

@router.get("/datablocks/{id}")
def get_datablocks_by_id(id:int):
    res = db_func(db.db_conn.get_datablocks_by_field,"data_id", id, "=")
    return res

@router.get("/datablocks/search/{string}")
def get_datablocks_by_search_string(string:str):
    res = db_func(db.db_conn.get_datablocks_by_search,"%" + string + "%")
    return res
