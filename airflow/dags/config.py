"""
Shared configuration for per-table Airflow DAGs.

Defines default args, path constants, and Airflow Dataset objects
for cross-DAG scheduling.

Paths and Spark connection are resolved via environment variables so the
same DAG files work both locally (airflow standalone) and in Docker:

    SPARK_JOBS_BASE  – root of spark/jobs/ on disk
                       default: <project_root>/spark/jobs  (local dev)
                       Docker:  /opt/spark/jobs

    SPARK_CONN_ID    – Airflow connection ID for SparkSubmitOperator
                       default: spark_default
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.datasets import Dataset

# ---------------------------------------------------------------------------
# Resolve project root (two levels up from this file: dags/ -> airflow/ -> project)
# ---------------------------------------------------------------------------
_PROJECT_ROOT = str(Path(__file__).resolve().parents[2])

# ---------------------------------------------------------------------------
# Default DAG arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Spark connection
# ---------------------------------------------------------------------------
SPARK_CONN_ID = os.environ.get("SPARK_CONN_ID", "spark_default")

# ---------------------------------------------------------------------------
# Shared SparkSubmitOperator settings
# ---------------------------------------------------------------------------
# Delta Lake packages for spark-submit --packages
SPARK_PACKAGES = "io.delta:delta-spark_2.12:3.3.0"

# Spark conf shared across all tasks (Delta extensions + project on PYTHONPATH)
SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.warehouse.dir": f"{_PROJECT_ROOT}/spark-warehouse",
}

# Environment vars passed to the Spark driver so `from spark.jobs...` imports work
SPARK_ENV_VARS = {
    "PYTHONPATH": _PROJECT_ROOT,
}

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------
SPARK_JOBS_BASE = os.environ.get("SPARK_JOBS_BASE", f"{_PROJECT_ROOT}/spark/jobs")

# Bronze
BRONZE_FEATURE_RELEASES_APP = f"{SPARK_JOBS_BASE}/bronze/feature_releases/main.py"
BRONZE_FEATURE_RELEASES_VALIDATE = f"{SPARK_JOBS_BASE}/bronze/feature_releases/validate.py"
BRONZE_USER_SIGNUPS_APP = f"{SPARK_JOBS_BASE}/bronze/user_signups/main.py"
BRONZE_USER_SIGNUPS_VALIDATE = f"{SPARK_JOBS_BASE}/bronze/user_signups/validate.py"
BRONZE_FEATURE_USAGE_EVENTS_APP = f"{SPARK_JOBS_BASE}/bronze/feature_usage_events/main.py"
BRONZE_FEATURE_USAGE_EVENTS_VALIDATE = f"{SPARK_JOBS_BASE}/bronze/feature_usage_events/validate.py"
BRONZE_CONVERSIONS_APP = f"{SPARK_JOBS_BASE}/bronze/conversions/main.py"
BRONZE_CONVERSIONS_VALIDATE = f"{SPARK_JOBS_BASE}/bronze/conversions/validate.py"

# Silver
SILVER_FEATURE_STATES_APP = f"{SPARK_JOBS_BASE}/silver/feature_states/main.py"
SILVER_FEATURE_STATES_VALIDATE = f"{SPARK_JOBS_BASE}/silver/feature_states/validate.py"
SILVER_USER_DIM_APP = f"{SPARK_JOBS_BASE}/silver/user_dim/main.py"
SILVER_USER_DIM_VALIDATE = f"{SPARK_JOBS_BASE}/silver/user_dim/validate.py"
SILVER_FEATURE_USAGE_FACTS_APP = f"{SPARK_JOBS_BASE}/silver/feature_usage_facts/main.py"
SILVER_FEATURE_USAGE_FACTS_VALIDATE = f"{SPARK_JOBS_BASE}/silver/feature_usage_facts/validate.py"

# Gold
GOLD_FEATURE_CONVERSION_IMPACT_APP = f"{SPARK_JOBS_BASE}/gold/feature_conversion_impact/main.py"
GOLD_FEATURE_CONVERSION_IMPACT_VALIDATE = (
    f"{SPARK_JOBS_BASE}/gold/feature_conversion_impact/validate.py"
)

# ---------------------------------------------------------------------------
# Airflow Datasets for cross-DAG scheduling
# ---------------------------------------------------------------------------
DATASET_BRONZE_FEATURE_RELEASES = Dataset("delta://bronze/feature_releases")
DATASET_BRONZE_USER_SIGNUPS = Dataset("delta://bronze/user_signups")
DATASET_BRONZE_FEATURE_USAGE_EVENTS = Dataset("delta://bronze/feature_usage_events")
DATASET_BRONZE_CONVERSIONS = Dataset("delta://bronze/conversions")

DATASET_SILVER_FEATURE_STATES = Dataset("delta://silver/feature_states")
DATASET_SILVER_USER_DIM = Dataset("delta://silver/user_dim")
DATASET_SILVER_FEATURE_USAGE_FACTS = Dataset("delta://silver/feature_usage_facts")

DATASET_GOLD_FEATURE_CONVERSION_IMPACT = Dataset("delta://gold/feature_conversion_impact")
