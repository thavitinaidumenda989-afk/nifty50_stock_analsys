import os
import yaml
import logging
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

logger = logging.getLogger("airflow.task")

# Complete filesystem path in Cloud Composer workers/schedulers
YAML_FILE_PATH = "/home/airflow/gcs/dags/config.yaml.yml"

# Safe default values
DEFAULT_PROJECT_ID = "menda-184002"
DEFAULT_LOCATION = "us-central1"

def load_and_validate(context):
    """Reads configuration from the complete local file path and validates bq_query."""
    project_id = DEFAULT_PROJECT_ID
    location = DEFAULT_LOCATION

    # Read config directly from the complete mounted path
    if os.path.exists(YAML_FILE_PATH):
        try:
            with open(YAML_FILE_PATH, "r") as f:
                cfg = yaml.safe_load(f)
                if cfg and "gcp" in cfg:
                    project_id = cfg["gcp"].get("project_id", DEFAULT_PROJECT_ID)
                    location = cfg["gcp"].get("region", DEFAULT_LOCATION)
                    logger.info(f"Successfully loaded configuration from: {YAML_FILE_PATH}")
        except Exception as e:
            logger.warning(f"Failed to parse {YAML_FILE_PATH}, using defaults. Error: {e}")
    else:
        logger.warning(f"File not found at {YAML_FILE_PATH}. Using default Project/Location.")

    # Validate query from Airflow variable
    query = Variable.get("bq_query", default_var="").strip()
    if not query:
        logger.error(f"[ERROR] Airflow variable 'bq_query' is missing or empty! Target: {project_id} ({location})")
        raise ValueError("Missing query in 'bq_query' variable.")

    logger.info("=" * 60)
    logger.info(f"Target Project ID : {project_id}")
    logger.info(f"Target Location   : {location}")
    logger.info("Query passed validation:")
    logger.info(f"\n{query}")
    logger.info("=" * 60)

default_args = {
    "owner": "data_engineering",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="adhoc_execute_bq_query",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["adhoc", "bigquery"],
) as dag:

    start_task = EmptyOperator(task_id="start")

    execute_queries = BigQueryInsertJobOperator(
        task_id="execute_bq_ddl_queries",
        project_id=DEFAULT_PROJECT_ID,
        location=DEFAULT_LOCATION,
        configuration={
            "query": {
                "query": "{{ var.value.get('bq_query', '') }}",
                "useLegacySql": False,
            }
        },
        pre_execute=load_and_validate,
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> execute_queries >> end_task