"""
Silver DAG: feature_states

Schedule: Triggered when bronze_feature_releases dataset is updated
Produces: DATASET_SILVER_FEATURE_STATES
"""

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config import (
    DATASET_BRONZE_FEATURE_RELEASES,
    DATASET_SILVER_FEATURE_STATES,
    DEFAULT_ARGS,
    SILVER_FEATURE_STATES_APP,
    SILVER_FEATURE_STATES_VALIDATE,
    SPARK_CONF,
    SPARK_CONN_ID,
    SPARK_ENV_VARS,
    SPARK_PACKAGES,
)

from airflow import DAG

with DAG(
    "silver_feature_states",
    default_args=DEFAULT_ARGS,
    description="SCD Type 2 feature states from bronze feature_releases",
    schedule=[DATASET_BRONZE_FEATURE_RELEASES],
    catchup=False,
) as dag:
    assert_input_quality = SparkSubmitOperator(
        task_id="assert_input_quality",
        conn_id=SPARK_CONN_ID,
        application=SILVER_FEATURE_STATES_VALIDATE,
        application_args=["--mode", "input"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    process = SparkSubmitOperator(
        task_id="process",
        conn_id=SPARK_CONN_ID,
        application=SILVER_FEATURE_STATES_APP,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        outlets=[DATASET_SILVER_FEATURE_STATES],
    )

    assert_output_quality = SparkSubmitOperator(
        task_id="assert_output_quality",
        conn_id=SPARK_CONN_ID,
        application=SILVER_FEATURE_STATES_VALIDATE,
        application_args=["--mode", "output"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    assert_input_quality >> process >> assert_output_quality
