-- Exécuté automatiquement au premier démarrage du container Postgres
-- (tout fichier .sql dans /docker-entrypoint-initdb.d/ est joué par l'image officielle)

-- Base dédiée aux données métier (séparée de la metadata Airflow)
-- NB: la DB `rfm` est déjà créée via POSTGRES_DB, on y crée juste les schémas.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Base dédiée à la metadata Airflow (séparée pour éviter tout mélange)
CREATE DATABASE airflow_meta;
