"""Helper de connexion Postgres — lu depuis les variables d'environnement."""
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_engine() -> Engine:
    user = os.environ["RFM_DB_USER"]
    password = os.environ["RFM_DB_PASSWORD"]
    host = os.environ["RFM_DB_HOST"]
    port = os.environ.get("RFM_DB_PORT", "5432")
    db = os.environ["RFM_DB_NAME"]
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, future=True)
