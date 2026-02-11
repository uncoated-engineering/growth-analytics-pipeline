"""
Bronze DAG: feature_usage_events

Schedule: @daily
Produces: DATASET_BRONZE_FEATURE_USAGE_EVENTS
"""

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config import (
    BRONZE_FEATURE_USAGE_EVENTS_APP,
    BRONZE_FEATURE_USAGE_EVENTS_VALIDATE,
    DATASET_BRONZE_FEATURE_USAGE_EVENTS,
    DEFAULT_ARGS,
    SPARK_CONF,
    SPARK_CONN_ID,
    SPARK_ENV_VARS,
    SPARK_PACKAGES,
)

from airflow import DAG

with DAG(
    "bronze_feature_usage_events",
    default_args=DEFAULT_ARGS,
    description="Ingest feature_usage_events raw JSONL into bronze Delta table",
    schedule="@daily",
    catchup=False,
) as dag:
    assert_input_quality = SparkSubmitOperator(
        task_id="assert_input_quality",
        conn_id=SPARK_CONN_ID,
        application=BRONZE_FEATURE_USAGE_EVENTS_VALIDATE,
        application_args=["--mode", "input"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    process = SparkSubmitOperator(
        task_id="process",
        conn_id=SPARK_CONN_ID,
        application=BRONZE_FEATURE_USAGE_EVENTS_APP,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        outlets=[DATASET_BRONZE_FEATURE_USAGE_EVENTS],
    )

    assert_output_quality = SparkSubmitOperator(
        task_id="assert_output_quality",
        conn_id=SPARK_CONN_ID,
        application=BRONZE_FEATURE_USAGE_EVENTS_VALIDATE,
        application_args=["--mode", "output"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    assert_input_quality >> process >> assert_output_quality
