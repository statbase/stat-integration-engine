import integrations.integrations as integrations
import db.db as db
from fastapi import FastAPI
from api.routes import router

app = FastAPI()
app.include_router(router, prefix="")
