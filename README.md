# Pipeline RFM — Docker & Airflow

Pipeline de données **RFM** (Recency / Frequency / Monetary) sur le dataset
[Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii),
orchestrée par **Airflow**, entièrement **dockerisée**, avec un dashboard
**Streamlit** pour la visualisation.

## 🏗 Architecture

Architecture **médaillon** dans PostgreSQL :

```
Excel  ──►  bronze.raw_online_retail  ──►  silver.cleaned_transactions  ──►  gold.rfm_scores  ──►  Streamlit
           (ingest.py)                     (transform.py)                    (rfm.py)
```

### Services Docker

| Service    | Rôle                                                       | Port   |
|------------|------------------------------------------------------------|--------|
| `postgres` | Metadata Airflow + données métier (bronze/silver/gold)     | 5432   |
| `airflow`  | Scheduler + Webserver (mode standalone, LocalExecutor)     | 8080   |
| `etl`      | Image Python custom, lancée par Airflow via DockerOperator | —      |
| `streamlit`| Dashboard de visualisation                                 | 8501   |

Airflow déclenche le container ETL via le **socket Docker** monté
(`/var/run/docker.sock`). Chaque tâche du DAG exécute une commande différente
dans l'image `rfm-etl:latest` (`python -m etl.ingest` / `etl.transform` / `etl.rfm`).

## 🚀 Lancement

### 1. Prérequis

- Docker Desktop
- Le dataset [`online_retail_II.xlsx`](https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip)
  placé dans `data/` (non versionné)

### 2. Configuration

```bash
cp .env.example .env
# Adapter AIRFLOW_UID avec : id -u
# Générer une Fernet key :
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Build & run

```bash
# Build de l'image ETL (service en profile, ne démarre pas automatiquement)
docker compose --profile build-only build etl

# Démarrage des services principaux
docker compose up -d
```

Patienter ~1 min que l'init Airflow finisse (`docker compose logs -f airflow-init`).

### 4. Accès aux UI

- **Airflow** : http://localhost:8080 — `admin` / `admin`
- **Streamlit** : http://localhost:8501

### 5. Exécuter le pipeline

1. Aller sur Airflow → DAG `rfm_pipeline` → activer (toggle) → **Trigger DAG**
2. Attendre la fin des 3 tâches (ingest → transform → compute_rfm)
3. Ouvrir Streamlit → le dashboard se peuple automatiquement

## 🧪 Vérification rapide

```bash
docker exec -it rfm_postgres psql -U airflow -d rfm -c "\dn"
docker exec -it rfm_postgres psql -U airflow -d rfm -c "SELECT COUNT(*) FROM bronze.raw_online_retail;"
docker exec -it rfm_postgres psql -U airflow -d rfm -c "SELECT * FROM gold.rfm_scores LIMIT 5;"
```

## 🧹 Nettoyage

```bash
docker compose down         # stop services (garde les données)
docker compose down -v      # stop + efface le volume Postgres (repart à zéro)
```

## 📝 Notes

- Le chemin du volume dataset est **hardcodé** dans [dags/rfm_dag.py](dags/rfm_dag.py)
  (`DATA_MOUNT.source`). À adapter selon la machine où tourne le projet.
- Le réseau Docker utilisé par `DockerOperator` est nommé `rfm-pipeline_default`
  (généré par compose à partir du nom du dossier).

## 🗂 Structure

```
.
├── docker-compose.yml
├── .env.example
├── sql/init.sql              # création des schémas bronze/silver/gold
├── dags/rfm_dag.py           # DAG Airflow (3 DockerOperator)
├── etl/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── db.py
│   ├── ingest.py             # bronze
│   ├── transform.py          # silver
│   └── rfm.py                # gold
├── streamlit_app/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
└── data/                     # dataset (non versionné)
```
