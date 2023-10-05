from fastapi import FastAPI
from api.routes import router
from config import configure_logger 


configure_logger.logger_config.configure_logging()
app = FastAPI()
app.include_router(router, prefix="")
