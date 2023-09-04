from fastapi import APIRouter
import objects.objects as objects
import db.db as db
import json
import integrations.integrations as integrations

router = APIRouter()

#TEST 
kolada = integrations.KoladaIntegration()
#/TEST

def db_select(select_func:callable, *args):
    conn = db.db_conn("db/stat-db.db")
    if args:
        res = select_func(conn, *args)
    else:
        res = select_func(conn)
    conn.close()
    return res

@router.get("/tags")
def get_datablocks_for_tags():
    res = db_select(db.db_conn.get_all_tags)
    return res

@router.get("/timeseries/id/{data_id}")
def get_timeseries_by_id(data_id:int):
    res = db_select(db.db_conn.get_timeseries_by_id, data_id)
    return json.loads(res.df.to_json(orient="records"))

@router.get("/datablocks/tag/{tag}")
def get_datablocks_by_tag(tag:str):
    res = db_select(db.db_conn.get_datablocks_by_field,"tags", "%"+tag+"%" ,"LIKE")
    return res

@router.get("/datablocks/id/{id}")
def get_datablocks_by_id(id:int):
    res = db_select(db.db_conn.get_datablocks_by_field,"data_id", id, "=")
    return res
