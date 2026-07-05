"""
Gold DAG: mrr_waterfall

Schedule: Triggered when the bronze_subscription_events dataset is updated
Produces: DATASET_GOLD_MRR_WATERFALL
"""

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config import (
    DATASET_BRONZE_SUBSCRIPTION_EVENTS,
    DATASET_GOLD_MRR_WATERFALL,
    DEFAULT_ARGS,
    GOLD_MRR_WATERFALL_APP,
    GOLD_MRR_WATERFALL_VALIDATE,
    SPARK_CONF,
    SPARK_CONN_ID,
    SPARK_ENV_VARS,
    SPARK_PACKAGES,
)

from airflow import DAG

with DAG(
    "gold_mrr_waterfall",
    default_args=DEFAULT_ARGS,
    description="Monthly MRR waterfall and net revenue retention",
    schedule=[DATASET_BRONZE_SUBSCRIPTION_EVENTS],
    catchup=False,
) as dag:
    assert_input_quality = SparkSubmitOperator(
        task_id="assert_input_quality",
        conn_id=SPARK_CONN_ID,
        application=GOLD_MRR_WATERFALL_VALIDATE,
        application_args=["--mode", "input"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    process = SparkSubmitOperator(
        task_id="process",
        conn_id=SPARK_CONN_ID,
        application=GOLD_MRR_WATERFALL_APP,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        outlets=[DATASET_GOLD_MRR_WATERFALL],
    )

    assert_output_quality = SparkSubmitOperator(
        task_id="assert_output_quality",
        conn_id=SPARK_CONN_ID,
        application=GOLD_MRR_WATERFALL_VALIDATE,
        application_args=["--mode", "output"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    assert_input_quality >> process >> assert_output_quality
