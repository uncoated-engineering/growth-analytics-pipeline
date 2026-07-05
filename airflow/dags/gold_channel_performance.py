"""
Gold DAG: channel_performance

Schedule: Triggered when silver_user_dim and bronze_conversions datasets are updated
Produces: DATASET_GOLD_CHANNEL_PERFORMANCE
"""

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config import (
    DATASET_BRONZE_CONVERSIONS,
    DATASET_GOLD_CHANNEL_PERFORMANCE,
    DATASET_SILVER_USER_DIM,
    DEFAULT_ARGS,
    GOLD_CHANNEL_PERFORMANCE_APP,
    GOLD_CHANNEL_PERFORMANCE_VALIDATE,
    SPARK_CONF,
    SPARK_CONN_ID,
    SPARK_ENV_VARS,
    SPARK_PACKAGES,
)

from airflow import DAG

with DAG(
    "gold_channel_performance",
    default_args=DEFAULT_ARGS,
    description="Acquisition channel performance per signup cohort",
    schedule=[DATASET_SILVER_USER_DIM, DATASET_BRONZE_CONVERSIONS],
    catchup=False,
) as dag:
    assert_input_quality = SparkSubmitOperator(
        task_id="assert_input_quality",
        conn_id=SPARK_CONN_ID,
        application=GOLD_CHANNEL_PERFORMANCE_VALIDATE,
        application_args=["--mode", "input"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    process = SparkSubmitOperator(
        task_id="process",
        conn_id=SPARK_CONN_ID,
        application=GOLD_CHANNEL_PERFORMANCE_APP,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        outlets=[DATASET_GOLD_CHANNEL_PERFORMANCE],
    )

    assert_output_quality = SparkSubmitOperator(
        task_id="assert_output_quality",
        conn_id=SPARK_CONN_ID,
        application=GOLD_CHANNEL_PERFORMANCE_VALIDATE,
        application_args=["--mode", "output"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    assert_input_quality >> process >> assert_output_quality
