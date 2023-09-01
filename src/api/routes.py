from fastapi import APIRouter
import objects.objects as objects
import db.db as db

router = APIRouter()
#Test
@router.get("/datablocks/category/{category}")
def get_datablocks_for_category(category:str):
    conn = db.db_conn("db/stat-db.db")
    res = conn.get_datablocks_by_category(category)
    return res

