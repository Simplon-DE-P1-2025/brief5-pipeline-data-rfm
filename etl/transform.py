"""
Étape SILVER — Nettoyage des données brutes.

Règles appliquées :
  - Drop lignes sans customer_id (pas exploitables pour le RFM)
  - Drop lignes avec quantity <= 0 (retours, annulations)
  - Drop lignes avec price <= 0
  - Drop factures commençant par 'C' (credit/annulation)
  - Calcul de la colonne `total_price = quantity * price`
  - Typage propre
"""
import logging

import pandas as pd
from sqlalchemy import text

from etl.db import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SRC = "bronze.raw_online_retail"
DST_SCHEMA = "silver"
DST_TABLE = "cleaned_transactions"


def run() -> None:
    engine = get_engine()
    log.info("Lecture de %s", SRC)
    df = pd.read_sql(f"SELECT * FROM {SRC}", engine)
    log.info("Lignes brutes : %d", len(df))

    before = len(df)
    df = df.dropna(subset=["customer_id"])
    df = df[df["quantity"] > 0]
    df = df[df["price"] > 0]
    df = df[~df["invoice"].astype(str).str.startswith("C")]
    log.info("Lignes après nettoyage : %d (supprimées : %d)", len(df), before - len(df))

    df["customer_id"] = df["customer_id"].astype(int)
    df["invoicedate"] = pd.to_datetime(df["invoicedate"])
    df["total_price"] = df["quantity"] * df["price"]

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {DST_SCHEMA}.{DST_TABLE}"))

    log.info("Écriture dans %s.%s", DST_SCHEMA, DST_TABLE)
    df.to_sql(
        DST_TABLE,
        engine,
        schema=DST_SCHEMA,
        if_exists="append",
        index=False,
        chunksize=10_000,
        method="multi",
    )
    log.info("Transformation terminée ✔")


if __name__ == "__main__":
    run()
