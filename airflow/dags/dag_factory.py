"""
Declarative DAG factory: auto-generates Airflow DAGs from pipeline.yaml.

Instead of maintaining 8 nearly-identical DAG files, this factory reads
the pipeline manifest and generates all DAGs dynamically. Each DAG follows
the same 3-task pattern:

    assert_input_quality -> process -> assert_output_quality

The factory preserves:
    - Per-table DAGs (not one monolithic DAG)
    - Dataset-based cross-DAG scheduling
    - SparkSubmitOperator for all tasks
    - The exact same task structure as the hand-written DAGs

Usage:
    # In an Airflow DAGs folder, create a single file:
    from dag_factory import create_all_dags
    dags = create_all_dags()
    # Airflow auto-discovers DAG objects in the module namespace
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.datasets import Dataset
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG

# ---------------------------------------------------------------------------
# Resolve project root and load manifest
# ---------------------------------------------------------------------------
_PROJECT_ROOT = str(Path(__file__).resolve().parents[2])

# We parse the YAML directly here to avoid import issues in Airflow's
# DAG parsing context (which has a restricted PYTHONPATH).
try:
    import yaml

    _MANIFEST_PATH = os.path.join(_PROJECT_ROOT, "pipeline.yaml")
    with open(_MANIFEST_PATH) as _f:
        _MANIFEST = yaml.safe_load(_f)
except Exception:
    _MANIFEST = None

# ---------------------------------------------------------------------------
# Shared configuration (mirrors config.py)
# ---------------------------------------------------------------------------
SPARK_CONN_ID = os.environ.get("SPARK_CONN_ID", "spark_default")
SPARK_PACKAGES = "io.delta:delta-spark_2.12:3.3.0"
SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.warehouse.dir": f"{_PROJECT_ROOT}/spark-warehouse",
}
SPARK_ENV_VARS = {"PYTHONPATH": _PROJECT_ROOT}
SPARK_JOBS_BASE = os.environ.get("SPARK_JOBS_BASE", f"{_PROJECT_ROOT}/spark/jobs")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _resolve_app_path(layer: str, table_name: str) -> str:
    """Resolve the main.py path for a table's Spark job."""
    return f"{SPARK_JOBS_BASE}/{layer}/{table_name}/main.py"


def _resolve_validate_path(layer: str, table_name: str) -> str:
    """Resolve the validate.py path for a table's quality checks."""
    return f"{SPARK_JOBS_BASE}/{layer}/{table_name}/validate.py"


def _build_schedule(airflow_config: dict) -> str | list:
    """Build the DAG schedule from manifest config."""
    schedule = airflow_config.get("schedule")
    if isinstance(schedule, str):
        return schedule
    if isinstance(schedule, dict):
        dataset_uris = schedule.get("datasets", [])
        return [Dataset(uri) for uri in dataset_uris]
    return "@daily"


def _build_outlets(airflow_config: dict) -> list:
    """Build Dataset outlets from manifest config."""
    dataset_uri = airflow_config.get("dataset")
    if dataset_uri:
        return [Dataset(dataset_uri)]
    return []


def create_dag_for_table(layer: str, table_name: str, table_config: dict) -> DAG | None:
    """Create a single DAG for a table from its manifest config.

    Returns:
        An Airflow DAG object, or None if airflow config is missing.
    """
    airflow_config = table_config.get("airflow", {})
    if not airflow_config:
        return None

    dag_id = f"{layer}_{table_name}"
    description = table_config.get("description", f"{layer} {table_name} pipeline")
    schedule = _build_schedule(airflow_config)
    outlets = _build_outlets(airflow_config)

    app_path = _resolve_app_path(layer, table_name)
    validate_path = _resolve_validate_path(layer, table_name)

    dag = DAG(
        dag_id,
        default_args=DEFAULT_ARGS,
        description=description,
        schedule=schedule,
        catchup=False,
    )

    with dag:
        assert_input_quality = SparkSubmitOperator(
            task_id="assert_input_quality",
            conn_id=SPARK_CONN_ID,
            application=validate_path,
            application_args=["--mode", "input"],
            packages=SPARK_PACKAGES,
            conf=SPARK_CONF,
            env_vars=SPARK_ENV_VARS,
        )

        extra_conf = {}
        if layer == "bronze":
            extra_conf = {"spark.executor.memory": "2g"}

        process = SparkSubmitOperator(
            task_id="process",
            conn_id=SPARK_CONN_ID,
            application=app_path,
            packages=SPARK_PACKAGES,
            conf={**SPARK_CONF, **extra_conf},
            env_vars=SPARK_ENV_VARS,
            outlets=outlets,
        )

        assert_output_quality = SparkSubmitOperator(
            task_id="assert_output_quality",
            conn_id=SPARK_CONN_ID,
            application=validate_path,
            application_args=["--mode", "output"],
            packages=SPARK_PACKAGES,
            conf=SPARK_CONF,
            env_vars=SPARK_ENV_VARS,
        )

        assert_input_quality >> process >> assert_output_quality

    return dag


def create_all_dags() -> dict[str, DAG]:
    """Create all DAGs from the pipeline manifest.

    Returns:
        Dict of dag_id -> DAG object.
    """
    if not _MANIFEST:
        return {}

    dags = {}
    for layer in ["bronze", "silver", "gold"]:
        layer_config = _MANIFEST.get(layer, {})
        for table_name, table_config in layer_config.items():
            dag = create_dag_for_table(layer, table_name, table_config)
            if dag:
                dags[dag.dag_id] = dag

    return dags


# ---------------------------------------------------------------------------
# Auto-create DAGs when this module is loaded by Airflow
# ---------------------------------------------------------------------------
_generated_dags = create_all_dags()
globals().update(_generated_dags)
