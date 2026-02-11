"""
Silver DAG: feature_usage_facts

Schedule: Triggered when bronze_feature_usage_events dataset is updated
Produces: DATASET_SILVER_FEATURE_USAGE_FACTS
"""

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config import (
    DATASET_BRONZE_FEATURE_USAGE_EVENTS,
    DATASET_SILVER_FEATURE_USAGE_FACTS,
    DEFAULT_ARGS,
    SILVER_FEATURE_USAGE_FACTS_APP,
    SILVER_FEATURE_USAGE_FACTS_VALIDATE,
    SPARK_CONF,
    SPARK_CONN_ID,
    SPARK_ENV_VARS,
    SPARK_PACKAGES,
)

from airflow import DAG

with DAG(
    "silver_feature_usage_facts",
    default_args=DEFAULT_ARGS,
    description="Feature usage facts aggregation from bronze feature_usage_events",
    schedule=[DATASET_BRONZE_FEATURE_USAGE_EVENTS],
    catchup=False,
) as dag:
    assert_input_quality = SparkSubmitOperator(
        task_id="assert_input_quality",
        conn_id=SPARK_CONN_ID,
        application=SILVER_FEATURE_USAGE_FACTS_VALIDATE,
        application_args=["--mode", "input"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    process = SparkSubmitOperator(
        task_id="process",
        conn_id=SPARK_CONN_ID,
        application=SILVER_FEATURE_USAGE_FACTS_APP,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        outlets=[DATASET_SILVER_FEATURE_USAGE_FACTS],
    )

    assert_output_quality = SparkSubmitOperator(
        task_id="assert_output_quality",
        conn_id=SPARK_CONN_ID,
        application=SILVER_FEATURE_USAGE_FACTS_VALIDATE,
        application_args=["--mode", "output"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    assert_input_quality >> process >> assert_output_quality
