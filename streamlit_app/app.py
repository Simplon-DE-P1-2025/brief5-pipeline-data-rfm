"""Dashboard Streamlit — visualisation du star schema gold."""
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine


@st.cache_resource
def get_engine():
    user = os.environ["RFM_DB_USER"]
    password = os.environ["RFM_DB_PASSWORD"]
    host = os.environ["RFM_DB_HOST"]
    port = os.environ.get("RFM_DB_PORT", "5432")
    db = os.environ["RFM_DB_NAME"]
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


@st.cache_data(ttl=60)
def load_rfm() -> pd.DataFrame:
    sql = """
        SELECT f.*, c.country
        FROM gold.fact_rfm_customer f
        JOIN gold.dim_customer c USING (customer_id)
    """
    return pd.read_sql(sql, get_engine())


@st.cache_data(ttl=60)
def load_sales_by_country() -> pd.DataFrame:
    sql = """
        SELECT c.country, SUM(f.line_total) AS revenue, COUNT(DISTINCT f.invoice_no) AS orders
        FROM gold.fact_order_line f
        JOIN gold.dim_customer c USING (customer_id)
        GROUP BY c.country
        ORDER BY revenue DESC
        LIMIT 15
    """
    return pd.read_sql(sql, get_engine())


@st.cache_data(ttl=60)
def load_top_products() -> pd.DataFrame:
    sql = """
        SELECT p.description, SUM(f.quantity) AS units, SUM(f.line_total) AS revenue
        FROM gold.fact_order_line f
        JOIN gold.dim_product p USING (product_id)
        GROUP BY p.description
        ORDER BY revenue DESC
        LIMIT 15
    """
    return pd.read_sql(sql, get_engine())


@st.cache_data(ttl=60)
def load_sales_by_month() -> pd.DataFrame:
    sql = """
        SELECT d.year, d.month, SUM(f.line_total) AS revenue
        FROM gold.fact_order_line f
        JOIN gold.dim_date d ON f.invoice_date_id = d.date_id
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month
    """
    df = pd.read_sql(sql, get_engine())
    df["period"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
    return df


# ---- UI -------------------------------------------------------------------
st.set_page_config(page_title="RFM Dashboard", layout="wide")
st.title("📊 RFM Dashboard — Online Retail II")
st.caption("Star schema : `fact_rfm_customer` + `fact_order_line` + dimensions partagées")

try:
    rfm = load_rfm()
except Exception as e:
    st.error(f"Impossible de charger gold.fact_rfm_customer : {e}")
    st.info("Lance d'abord le DAG `rfm_pipeline` dans Airflow (http://localhost:8080).")
    st.stop()

# ---- KPIs -----------------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)
c1.metric("Clients", f"{len(rfm):,}")
c2.metric("Revenu RFM total", f"£{rfm['monetary'].sum():,.0f}")
c3.metric("Panier moyen", f"£{rfm['monetary'].mean():,.2f}")
c4.metric("Fréquence moy.", f"{rfm['frequency'].mean():.1f}")

st.divider()

# ---- Segmentation RFM -----------------------------------------------------
st.subheader("🎯 Segmentation RFM")
col1, col2 = st.columns(2)
with col1:
    seg_counts = rfm["segment"].value_counts().reset_index()
    seg_counts.columns = ["segment", "count"]
    st.plotly_chart(
        px.bar(seg_counts, x="segment", y="count", color="segment",
               title="Répartition des clients par segment"),
        use_container_width=True,
    )
with col2:
    st.plotly_chart(
        px.scatter(
            rfm, x="recency", y="frequency", size="monetary", color="segment",
            hover_data=["customer_id", "rfm_score", "country"], log_y=True,
            title="Recency × Frequency × Monetary",
        ),
        use_container_width=True,
    )

# ---- Sales analytics (via fact_order_line) --------------------------------
st.divider()
st.subheader("💰 Analyses ventes (via fact_order_line)")

col3, col4 = st.columns(2)
with col3:
    by_country = load_sales_by_country()
    st.plotly_chart(
        px.bar(by_country, x="country", y="revenue", title="Top 15 pays par revenu"),
        use_container_width=True,
    )
with col4:
    by_month = load_sales_by_month()
    st.plotly_chart(
        px.line(by_month, x="period", y="revenue", markers=True,
                title="Évolution mensuelle du revenu"),
        use_container_width=True,
    )

st.subheader("🏆 Top 15 produits (revenu)")
st.dataframe(load_top_products(), use_container_width=True)

# ---- Top clients ----------------------------------------------------------
st.divider()
st.subheader("⭐ Top 20 clients (Monetary)")
top_cols = ["customer_id", "country", "recency", "frequency", "monetary",
            "rfm_score", "segment"]
st.dataframe(rfm.nlargest(20, "monetary")[top_cols], use_container_width=True)

with st.expander("Voir tous les clients RFM"):
    st.dataframe(rfm, use_container_width=True)
