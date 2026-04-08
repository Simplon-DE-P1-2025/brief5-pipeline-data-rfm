"""
Étape GOLD — Construction du star schema.

Produit 5 tables dans le schéma `gold` :
  - dim_customer        : 1 ligne par client
  - dim_product         : 1 ligne par produit (stock_code)
  - dim_date            : 1 ligne par date présente dans les transactions
  - fact_order_line     : 1 ligne par produit dans une facture
  - fact_rfm_customer   : 1 ligne par client à la date de calcul (snapshot)
"""
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from etl.db import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SRC = "silver.cleaned_transactions"
SCHEMA = "gold"


def _write(df: pd.DataFrame, name: str, engine) -> None:
    log.info("Écriture %s.%s (%d lignes)", SCHEMA, name, len(df))
    df.to_sql(
        name, engine, schema=SCHEMA, if_exists="replace",
        index=False, chunksize=10_000, method="multi",
    )


def run() -> None:
    engine = get_engine()

    # S'assurer que le schéma existe (utile si on a recréé la DB)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))

    log.info("Lecture de %s", SRC)
    df = pd.read_sql(
        f"SELECT invoice, stockcode, description, quantity, invoicedate, "
        f"price, customer_id, country, total_price FROM {SRC}",
        engine,
    )
    df["invoicedate"] = pd.to_datetime(df["invoicedate"])
    log.info("Lignes silver : %d", len(df))

    # ---------- dim_customer -------------------------------------------------
    dim_customer = (
        df.groupby("customer_id", as_index=False)["country"]
          .agg(lambda s: s.mode().iloc[0])  # pays le plus fréquent
    )
    _write(dim_customer, "dim_customer", engine)

    # ---------- dim_product --------------------------------------------------
    # 1 ligne par stock_code (description la plus fréquente). product_id = surrogate.
    dim_product = (
        df.dropna(subset=["description"])
          .groupby("stockcode", as_index=False)["description"]
          .agg(lambda s: s.mode().iloc[0])
          .rename(columns={"stockcode": "stock_code"})
    )
    dim_product.insert(0, "product_id", range(1, len(dim_product) + 1))
    _write(dim_product, "dim_product", engine)

    # ---------- dim_date -----------------------------------------------------
    dates = pd.DataFrame({"full_date": pd.to_datetime(df["invoicedate"].dt.date.unique())})
    dates = dates.sort_values("full_date").reset_index(drop=True)
    dates.insert(0, "date_id", range(1, len(dates) + 1))
    dates["day"] = dates["full_date"].dt.day
    dates["month"] = dates["full_date"].dt.month
    dates["year"] = dates["full_date"].dt.year
    dates["quarter"] = dates["full_date"].dt.quarter
    _write(dates, "dim_date", engine)

    # ---------- fact_order_line ---------------------------------------------
    # 1 ligne = 1 produit dans 1 facture
    prod_lookup = dim_product.set_index("stock_code")["product_id"].to_dict()
    date_lookup = dates.set_index("full_date")["date_id"].to_dict()

    fact_ol = pd.DataFrame({
        "order_line_id": range(1, len(df) + 1),
        "invoice_no": df["invoice"].astype(str).values,
        "customer_id": df["customer_id"].values,
        "product_id": df["stockcode"].map(prod_lookup).values,
        "invoice_date_id": pd.to_datetime(df["invoicedate"].dt.date).map(date_lookup).values,
        "quantity": df["quantity"].values,
        "unit_price": df["price"].values,
        "line_total": df["total_price"].values,
    })
    _write(fact_ol, "fact_order_line", engine)

    # ---------- fact_rfm_customer -------------------------------------------
    snapshot_date = pd.Timestamp(datetime.utcnow().date())
    ref_date = df["invoicedate"].max() + pd.Timedelta(days=1)
    log.info("Snapshot RFM : %s (date de référence : %s)", snapshot_date.date(), ref_date.date())

    rfm = df.groupby("customer_id").agg(
        recency=("invoicedate", lambda s: (ref_date - s.max()).days),
        frequency=("invoice", "nunique"),
        monetary=("total_price", "sum"),
    ).reset_index()

    rfm["recency_score"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1]).astype(int)
    rfm["frequency_score"] = pd.qcut(
        rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]
    ).astype(int)
    rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5]).astype(int)
    rfm["rfm_score"] = (
        rfm["recency_score"].astype(str)
        + rfm["frequency_score"].astype(str)
        + rfm["monetary_score"].astype(str)
    )

    def segment(row) -> str:
        r, f, m = row["recency_score"], row["frequency_score"], row["monetary_score"]
        if r >= 4 and f >= 4 and m >= 4:
            return "Champions"
        if r >= 4 and f >= 3:
            return "Loyal"
        if r >= 4:
            return "Recent"
        if f >= 4 and m >= 4:
            return "At Risk"
        if r <= 2 and f <= 2:
            return "Lost"
        return "Others"

    rfm["segment"] = rfm.apply(segment, axis=1)

    # snapshot_date_id : on ajoute la date du snapshot dans dim_date si absente
    if snapshot_date not in date_lookup:
        new_id = max(date_lookup.values()) + 1
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"INSERT INTO {SCHEMA}.dim_date "
                    f"(date_id, full_date, day, month, year, quarter) "
                    f"VALUES (:id, :d, :day, :m, :y, :q)"
                ),
                {
                    "id": new_id, "d": snapshot_date.date(),
                    "day": snapshot_date.day, "m": snapshot_date.month,
                    "y": snapshot_date.year, "q": (snapshot_date.month - 1) // 3 + 1,
                },
            )
        date_lookup[snapshot_date] = new_id

    rfm.insert(0, "rfm_id", range(1, len(rfm) + 1))
    rfm["snapshot_date_id"] = date_lookup[snapshot_date]

    cols = [
        "rfm_id", "customer_id", "snapshot_date_id",
        "recency", "frequency", "monetary",
        "recency_score", "frequency_score", "monetary_score",
        "rfm_score", "segment",
    ]
    _write(rfm[cols], "fact_rfm_customer", engine)

    log.info("Star schema construit ✔")


if __name__ == "__main__":
    run()
