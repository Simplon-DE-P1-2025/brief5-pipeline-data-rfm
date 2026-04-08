"""
Étape BRONZE — Ingestion brute du dataset Online Retail II dans Postgres.

Lit le fichier Excel (2 feuilles: 2009-2010 et 2010-2011), les concatène
et charge le tout dans `bronze.raw_online_retail` SANS transformation.
"""
import logging
from pathlib import Path

import pandas as pd

from etl.db import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATA_FILE = Path("/app/data/online_retail_II.xlsx")
TABLE = "raw_online_retail"
SCHEMA = "bronze"


def run() -> None:
    log.info("Lecture du fichier Excel %s", DATA_FILE)
    # Le fichier contient 2 feuilles, on les concatène
    sheets = pd.read_excel(DATA_FILE, sheet_name=None, engine="openpyxl")
    df = pd.concat(sheets.values(), ignore_index=True)
    log.info("Lignes lues : %d", len(df))

    # Normalisation des noms de colonnes (snake_case, pas d'espaces)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    engine = get_engine()
    log.info("Écriture dans %s.%s (replace)", SCHEMA, TABLE)
    df.to_sql(
        TABLE,
        engine,
        schema=SCHEMA,
        if_exists="replace",
        index=False,
        chunksize=10_000,
        method="multi",
    )
    log.info("Ingestion terminée ✔")


if __name__ == "__main__":
    run()
