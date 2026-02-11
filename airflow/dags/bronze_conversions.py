"""
Bronze DAG: conversions

Schedule: @daily
Produces: DATASET_BRONZE_CONVERSIONS
"""

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config import (
    BRONZE_CONVERSIONS_APP,
    BRONZE_CONVERSIONS_VALIDATE,
    DATASET_BRONZE_CONVERSIONS,
    DEFAULT_ARGS,
    SPARK_CONF,
    SPARK_CONN_ID,
    SPARK_ENV_VARS,
    SPARK_PACKAGES,
)

from airflow import DAG

with DAG(
    "bronze_conversions",
    default_args=DEFAULT_ARGS,
    description="Ingest conversions raw JSONL into bronze Delta table",
    schedule="@daily",
    catchup=False,
) as dag:
    assert_input_quality = SparkSubmitOperator(
        task_id="assert_input_quality",
        conn_id=SPARK_CONN_ID,
        application=BRONZE_CONVERSIONS_VALIDATE,
        application_args=["--mode", "input"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    process = SparkSubmitOperator(
        task_id="process",
        conn_id=SPARK_CONN_ID,
        application=BRONZE_CONVERSIONS_APP,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        outlets=[DATASET_BRONZE_CONVERSIONS],
    )

    assert_output_quality = SparkSubmitOperator(
        task_id="assert_output_quality",
        conn_id=SPARK_CONN_ID,
        application=BRONZE_CONVERSIONS_VALIDATE,
        application_args=["--mode", "output"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    assert_input_quality >> process >> assert_output_quality
