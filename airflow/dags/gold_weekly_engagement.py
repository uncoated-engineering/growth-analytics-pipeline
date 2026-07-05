"""
Gold DAG: weekly_engagement

Schedule: Triggered when the bronze_feature_usage_events dataset is updated
Produces: DATASET_GOLD_WEEKLY_ENGAGEMENT
"""

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config import (
    DATASET_BRONZE_FEATURE_USAGE_EVENTS,
    DATASET_GOLD_WEEKLY_ENGAGEMENT,
    DEFAULT_ARGS,
    GOLD_WEEKLY_ENGAGEMENT_APP,
    GOLD_WEEKLY_ENGAGEMENT_VALIDATE,
    SPARK_CONF,
    SPARK_CONN_ID,
    SPARK_ENV_VARS,
    SPARK_PACKAGES,
)

from airflow import DAG

with DAG(
    "gold_weekly_engagement",
    default_args=DEFAULT_ARGS,
    description="Weekly active users and feature adoption metrics",
    schedule=[DATASET_BRONZE_FEATURE_USAGE_EVENTS],
    catchup=False,
) as dag:
    assert_input_quality = SparkSubmitOperator(
        task_id="assert_input_quality",
        conn_id=SPARK_CONN_ID,
        application=GOLD_WEEKLY_ENGAGEMENT_VALIDATE,
        application_args=["--mode", "input"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    process = SparkSubmitOperator(
        task_id="process",
        conn_id=SPARK_CONN_ID,
        application=GOLD_WEEKLY_ENGAGEMENT_APP,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        outlets=[DATASET_GOLD_WEEKLY_ENGAGEMENT],
    )

    assert_output_quality = SparkSubmitOperator(
        task_id="assert_output_quality",
        conn_id=SPARK_CONN_ID,
        application=GOLD_WEEKLY_ENGAGEMENT_VALIDATE,
        application_args=["--mode", "output"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    assert_input_quality >> process >> assert_output_quality
