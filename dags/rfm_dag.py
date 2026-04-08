"""
DAG RFM — orchestration de la pipeline médaillon (bronze → silver → gold).

Chaque tâche lance le container `rfm-etl:latest` via DockerOperator avec une
commande différente. Les containers lancés par Airflow rejoignent le réseau
Docker de compose pour pouvoir atteindre le service `postgres` par son nom.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Nom du réseau Docker créé automatiquement par docker compose.
# Format : <nom_du_dossier>_default → ici "rfm-pipeline_default".
# À adapter si le dossier du projet a un autre nom.
COMPOSE_NETWORK = "rfm-pipeline_default"

ETL_IMAGE = "rfm-etl:latest"

# Variables d'environnement passées au container ETL à chaque run.
# (Airflow les récupère depuis son propre environnement, injecté par compose.)
ETL_ENV = {
    "RFM_DB_HOST": "postgres",
    "RFM_DB_PORT": "5432",
    "RFM_DB_USER": "{{ var.value.get('RFM_DB_USER', 'airflow') }}",
    "RFM_DB_PASSWORD": "{{ var.value.get('RFM_DB_PASSWORD', 'airflow') }}",
    "RFM_DB_NAME": "rfm",
}

# Volume du dataset monté en read-only dans le container ETL.
# Le chemin SOURCE est celui de l'HÔTE (pas du container Airflow) car c'est
# le démon Docker de l'hôte qui crée le container ETL via le socket monté.
# Lu depuis la variable HOST_DATA_PATH du fichier .env (à adapter par chacun).
DATA_MOUNT = Mount(
    source=os.environ["HOST_DATA_PATH"],
    target="/app/data",
    type="bind",
    read_only=True,
)

default_args = {
    "owner": "data-team",
    "retries": 0,
}

with DAG(
    dag_id="rfm_pipeline",
    description="Pipeline RFM : ingestion → transformation → scoring",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,  # déclenchement manuel
    catchup=False,
    tags=["rfm", "medallion"],
) as dag:

    ingest = DockerOperator(
        task_id="ingest_bronze",
        image=ETL_IMAGE,
        command="python -m etl.ingest",
        auto_remove="success",
        network_mode=COMPOSE_NETWORK,
        mounts=[DATA_MOUNT],
        environment=ETL_ENV,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
    )

    transform = DockerOperator(
        task_id="transform_silver",
        image=ETL_IMAGE,
        command="python -m etl.transform",
        auto_remove="success",
        network_mode=COMPOSE_NETWORK,
        environment=ETL_ENV,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
    )

    score = DockerOperator(
        task_id="build_gold_star_schema",
        image=ETL_IMAGE,
        command="python -m etl.rfm",
        auto_remove="success",
        network_mode=COMPOSE_NETWORK,
        environment=ETL_ENV,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
    )

    ingest >> transform >> score
