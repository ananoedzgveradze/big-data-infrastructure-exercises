from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from bdi_api.settings import DBCredentials

db_credentials = DBCredentials()

DATABASE_URL = f"postgresql://{db_credentials.user}:{db_credentials.password}@{db_credentials.host}:{db_credentials.port}/{db_credentials.database}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close() 