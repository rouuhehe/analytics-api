from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient
import os

# -----------------------
# PostgreSQL (MS1)
# -----------------------
PG_DB_URL = os.getenv(
    "PG_DB_URL",
    "postgresql://ms1user:ms1pass@98.90.233.171:5432/ms1db"
)
pg_engine = create_engine(PG_DB_URL)
PGSession = sessionmaker(bind=pg_engine)

def get_pg_db():
    db = PGSession()
    try:
        yield db
    finally:
        db.close()

# -----------------------
# MySQL (MS2)
# -----------------------
MYSQL_DB_URL = os.getenv(
    "MYSQL_DB_URL",
    "mysql+pymysql://ms2user:ms2pass@98.90.233.171:3306/ms2db"
)
mysql_engine = create_engine(MYSQL_DB_URL)
MySQLSession = sessionmaker(bind=mysql_engine)

def get_mysql_db():
    db = MySQLSession()
    try:
        yield db
    finally:
        db.close()

# -----------------------
# MongoDB (MS3)
# -----------------------
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:rootpass@98.90.233.171:27017/historias?authSource=admin"
)
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client["historias"]
histories_collection = mongo_db["histories"]
