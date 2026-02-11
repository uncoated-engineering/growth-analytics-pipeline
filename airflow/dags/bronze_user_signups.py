"""
Bronze DAG: user_signups

Schedule: @daily
Produces: DATASET_BRONZE_USER_SIGNUPS
"""

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config import (
    BRONZE_USER_SIGNUPS_APP,
    BRONZE_USER_SIGNUPS_VALIDATE,
    DATASET_BRONZE_USER_SIGNUPS,
    DEFAULT_ARGS,
    SPARK_CONF,
    SPARK_CONN_ID,
    SPARK_ENV_VARS,
    SPARK_PACKAGES,
)

from airflow import DAG

with DAG(
    "bronze_user_signups",
    default_args=DEFAULT_ARGS,
    description="Ingest user_signups raw JSONL into bronze Delta table",
    schedule="@daily",
    catchup=False,
) as dag:
    assert_input_quality = SparkSubmitOperator(
        task_id="assert_input_quality",
        conn_id=SPARK_CONN_ID,
        application=BRONZE_USER_SIGNUPS_VALIDATE,
        application_args=["--mode", "input"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    process = SparkSubmitOperator(
        task_id="process",
        conn_id=SPARK_CONN_ID,
        application=BRONZE_USER_SIGNUPS_APP,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        outlets=[DATASET_BRONZE_USER_SIGNUPS],
    )

    assert_output_quality = SparkSubmitOperator(
        task_id="assert_output_quality",
        conn_id=SPARK_CONN_ID,
        application=BRONZE_USER_SIGNUPS_VALIDATE,
        application_args=["--mode", "output"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    assert_input_quality >> process >> assert_output_quality
