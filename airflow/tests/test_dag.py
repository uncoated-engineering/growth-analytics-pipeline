"""
Tests for the SaaS PLG Analytics Pipeline DAG.

Validates DAG structure, task dependencies, default args, and schedule.
"""

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    """Load DAGs from the dags directory."""
    return DagBag(dag_folder="airflow/dags", include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag):
    """Get the saas_plg_analytics_pipeline DAG."""
    dag_id = "saas_plg_analytics_pipeline"
    assert dag_id in dagbag.dags, f"DAG '{dag_id}' not found. Errors: {dagbag.import_errors}"
    return dagbag.dags[dag_id]


class TestDagLoading:
    """Verify the DAG loads without errors."""

    def test_no_import_errors(self, dagbag):
        assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"

    def test_dag_loaded(self, dag):
        assert dag is not None
        assert dag.dag_id == "saas_plg_analytics_pipeline"


class TestDagConfiguration:
    """Verify DAG-level configuration matches the spec."""

    def test_description(self, dag):
        assert dag.description == "SaaS Product-Led Growth Analytics Pipeline"

    def test_schedule(self, dag):
        assert str(dag.timetable) == "CronTriggerTimetable('@daily')" or dag.schedule == "@daily"

    def test_catchup_disabled(self, dag):
        assert dag.catchup is False

    def test_default_args_owner(self, dag):
        assert dag.default_args["owner"] == "data-engineering"

    def test_default_args_retries(self, dag):
        assert dag.default_args["retries"] == 1


class TestDagTasks:
    """Verify all expected tasks exist."""

    EXPECTED_TASKS = [
        "ingest_feature_releases",
        "ingest_user_signups",
        "ingest_feature_usage",
        "ingest_conversions",
        "maintain_feature_states_scd",
        "build_feature_usage_facts",
        "calculate_feature_conversion_impact",
    ]

    def test_task_count(self, dag):
        assert len(dag.tasks) == 7

    @pytest.mark.parametrize("task_id", EXPECTED_TASKS)
    def test_task_exists(self, dag, task_id):
        assert dag.has_task(task_id), f"Task '{task_id}' not found in DAG"


class TestDagDependencies:
    """Verify task dependency graph matches the spec."""

    def test_bronze_tasks_feed_into_scd(self, dag):
        """All four bronze ingestion tasks must complete before SCD starts."""
        scd_task = dag.get_task("maintain_feature_states_scd")
        upstream_ids = {t.task_id for t in scd_task.upstream_list}
        assert upstream_ids == {
            "ingest_feature_releases",
            "ingest_user_signups",
            "ingest_feature_usage",
            "ingest_conversions",
        }

    def test_scd_before_usage_facts(self, dag):
        """SCD must complete before usage facts are built."""
        usage_task = dag.get_task("build_feature_usage_facts")
        upstream_ids = {t.task_id for t in usage_task.upstream_list}
        assert upstream_ids == {"maintain_feature_states_scd"}

    def test_usage_facts_before_gold(self, dag):
        """Usage facts must complete before gold analysis runs."""
        gold_task = dag.get_task("calculate_feature_conversion_impact")
        upstream_ids = {t.task_id for t in gold_task.upstream_list}
        assert upstream_ids == {"build_feature_usage_facts"}

    def test_bronze_tasks_have_no_upstream(self, dag):
        """Bronze ingestion tasks should have no upstream dependencies."""
        for task_id in [
            "ingest_feature_releases",
            "ingest_user_signups",
            "ingest_feature_usage",
            "ingest_conversions",
        ]:
            task = dag.get_task(task_id)
            assert len(task.upstream_list) == 0, f"{task_id} has unexpected upstream tasks"

    def test_gold_task_has_no_downstream(self, dag):
        """Gold analysis task should be the terminal task."""
        gold_task = dag.get_task("calculate_feature_conversion_impact")
        assert len(gold_task.downstream_list) == 0


class TestSparkSubmitConfig:
    """Verify SparkSubmitOperator configurations."""

    def test_ingest_features_has_executor_memory(self, dag):
        task = dag.get_task("ingest_feature_releases")
        conf = getattr(task, "conf", None) or getattr(task, "_conf", None)
        assert conf is not None
        assert conf.get("spark.executor.memory") == "2g"

    def test_all_tasks_use_spark_submit(self, dag):
        from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

        for task in dag.tasks:
            assert isinstance(
                task, SparkSubmitOperator
            ), f"Task '{task.task_id}' is not a SparkSubmitOperator"
