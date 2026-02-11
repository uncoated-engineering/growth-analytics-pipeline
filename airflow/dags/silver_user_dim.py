"""
Silver DAG: user_dim

Schedule: Triggered when bronze_user_signups and bronze_conversions datasets are updated
Produces: DATASET_SILVER_USER_DIM
"""

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config import (
    DATASET_BRONZE_CONVERSIONS,
    DATASET_BRONZE_USER_SIGNUPS,
    DATASET_SILVER_USER_DIM,
    DEFAULT_ARGS,
    SILVER_USER_DIM_APP,
    SILVER_USER_DIM_VALIDATE,
    SPARK_CONF,
    SPARK_CONN_ID,
    SPARK_ENV_VARS,
    SPARK_PACKAGES,
)

from airflow import DAG

with DAG(
    "silver_user_dim",
    default_args=DEFAULT_ARGS,
    description="User dimension table from bronze user_signups and conversions",
    schedule=[DATASET_BRONZE_USER_SIGNUPS, DATASET_BRONZE_CONVERSIONS],
    catchup=False,
) as dag:
    assert_input_quality = SparkSubmitOperator(
        task_id="assert_input_quality",
        conn_id=SPARK_CONN_ID,
        application=SILVER_USER_DIM_VALIDATE,
        application_args=["--mode", "input"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    process = SparkSubmitOperator(
        task_id="process",
        conn_id=SPARK_CONN_ID,
        application=SILVER_USER_DIM_APP,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        outlets=[DATASET_SILVER_USER_DIM],
    )

    assert_output_quality = SparkSubmitOperator(
        task_id="assert_output_quality",
        conn_id=SPARK_CONN_ID,
        application=SILVER_USER_DIM_VALIDATE,
        application_args=["--mode", "output"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    assert_input_quality >> process >> assert_output_quality
