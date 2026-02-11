"""
SaaS Product-Led Growth Analytics Pipeline - Airflow DAG

Orchestrates the medallion architecture pipeline:
  Bronze (parallel ingestion) → Silver (SCD + aggregation) → Gold (cohort analysis)

Schedule: Daily
"""

from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "saas_plg_analytics_pipeline",
    default_args=default_args,
    description="SaaS Product-Led Growth Analytics Pipeline",
    schedule="@daily",
    catchup=False,
)

# ---------------------------------------------------------------------------
# Bronze ingestion tasks (run in parallel)
# ---------------------------------------------------------------------------
ingest_features = SparkSubmitOperator(
    task_id="ingest_feature_releases",
    application="/opt/spark/jobs/bronze/main.py",
    conf={"spark.executor.memory": "2g"},
    dag=dag,
)

ingest_users = SparkSubmitOperator(
    task_id="ingest_user_signups",
    application="/opt/spark/jobs/bronze/main.py",
    dag=dag,
)

ingest_usage = SparkSubmitOperator(
    task_id="ingest_feature_usage",
    application="/opt/spark/jobs/bronze/main.py",
    dag=dag,
)

ingest_conversions = SparkSubmitOperator(
    task_id="ingest_conversions",
    application="/opt/spark/jobs/bronze/main.py",
    dag=dag,
)

# ---------------------------------------------------------------------------
# Silver transformation tasks
# ---------------------------------------------------------------------------
maintain_scd = SparkSubmitOperator(
    task_id="maintain_feature_states_scd",
    application="/opt/spark/jobs/silver/main.py",
    dag=dag,
)

build_usage_facts = SparkSubmitOperator(
    task_id="build_feature_usage_facts",
    application="/opt/spark/jobs/silver/main.py",
    dag=dag,
)

# ---------------------------------------------------------------------------
# Gold analytics tasks
# ---------------------------------------------------------------------------
analyze_feature_impact = SparkSubmitOperator(
    task_id="calculate_feature_conversion_impact",
    application="/opt/spark/jobs/gold/main.py",
    dag=dag,
)

# ---------------------------------------------------------------------------
# Define dependencies
# ---------------------------------------------------------------------------
# Bronze layer runs in parallel
[ingest_features, ingest_users, ingest_usage, ingest_conversions] >> maintain_scd

# Silver layer (SCD must complete before usage facts)
maintain_scd >> build_usage_facts

# Gold layer (needs both SCD and usage facts)
build_usage_facts >> analyze_feature_impact
