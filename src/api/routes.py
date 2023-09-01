from fastapi import APIRouter
import objects.objects as objects
import db.db as db
import json

router = APIRouter()
#Test
@router.get("/datablocks/category/{title}")
def get_datablocks_for_category(title:str):
    conn = db.db_conn("db/stat-db.db")
    res = conn.get_datablocks_by_field(name="category", val=title)
    conn.close()
    return res

@router.get("/timeseries/id/{data_id}")
def get_timeseries_by_id(data_id:int):
    conn = db.db_conn("db/stat-db.db")
    res = conn.get_timeseries_by_id(data_id)
    conn.close()
    return json.loads(res.to_json(orient="records"))

@router.get("/categories/source/{source}")
def get_categories_by_source(source:str):
    conn = db.db_conn("db/stat-db.db")
    res = conn.get_categories_by_source(source)
    conn.close()
    return json.loads(res.to_json(orient="records"))

