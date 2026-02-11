"""
Gold DAG: feature_conversion_impact

Schedule: Triggered when all upstream silver + bronze_conversions datasets are updated
Produces: DATASET_GOLD_FEATURE_CONVERSION_IMPACT
"""

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config import (
    DATASET_BRONZE_CONVERSIONS,
    DATASET_GOLD_FEATURE_CONVERSION_IMPACT,
    DATASET_SILVER_FEATURE_STATES,
    DATASET_SILVER_FEATURE_USAGE_FACTS,
    DATASET_SILVER_USER_DIM,
    DEFAULT_ARGS,
    GOLD_FEATURE_CONVERSION_IMPACT_APP,
    GOLD_FEATURE_CONVERSION_IMPACT_VALIDATE,
    SPARK_CONF,
    SPARK_CONN_ID,
    SPARK_ENV_VARS,
    SPARK_PACKAGES,
)

from airflow import DAG

with DAG(
    "gold_feature_conversion_impact",
    default_args=DEFAULT_ARGS,
    description="Feature conversion impact cohort analysis",
    schedule=[
        DATASET_SILVER_USER_DIM,
        DATASET_SILVER_FEATURE_STATES,
        DATASET_SILVER_FEATURE_USAGE_FACTS,
        DATASET_BRONZE_CONVERSIONS,
    ],
    catchup=False,
) as dag:
    assert_input_quality = SparkSubmitOperator(
        task_id="assert_input_quality",
        conn_id=SPARK_CONN_ID,
        application=GOLD_FEATURE_CONVERSION_IMPACT_VALIDATE,
        application_args=["--mode", "input"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    process = SparkSubmitOperator(
        task_id="process",
        conn_id=SPARK_CONN_ID,
        application=GOLD_FEATURE_CONVERSION_IMPACT_APP,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        outlets=[DATASET_GOLD_FEATURE_CONVERSION_IMPACT],
    )

    assert_output_quality = SparkSubmitOperator(
        task_id="assert_output_quality",
        conn_id=SPARK_CONN_ID,
        application=GOLD_FEATURE_CONVERSION_IMPACT_VALIDATE,
        application_args=["--mode", "output"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
    )

    assert_input_quality >> process >> assert_output_quality
