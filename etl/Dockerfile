FROM python:3.11-slim

WORKDIR /app

# Dépendances système minimales (pour psycopg2-binary ça suffit)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie des scripts ETL dans l'image en tant que package `etl`
COPY . /app/etl
ENV PYTHONPATH=/app

# Par défaut on ne fait rien : Airflow passe la commande réelle
# (ex: python -m etl.ingest) via DockerOperator.
CMD ["python", "--version"]
