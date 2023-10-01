from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from config import config

SQLALCHEMY_DATABASE_URL = f"sqlite:///{config.get('DB_STRING')}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)

session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Session = scoped_session(session_factory)

Base = declarative_base()


