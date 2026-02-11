"""
Tests for the per-table Airflow DAGs.

Validates that all 8 DAGs load correctly, each has the expected 3-task
chain (assert_input_quality -> process -> assert_output_quality),
and that cross-DAG Dataset dependencies are configured correctly.
"""

import os
import sys

import pytest
from airflow.models import DagBag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DAGS_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")


@pytest.fixture(scope="module")
def dagbag():
    """Load DAGs from the dags directory."""
    # Ensure the dags folder is on sys.path so that `from config import ...` works.
    # In production Airflow, the configured dags_folder is added automatically.
    abs_dags = os.path.abspath(DAGS_FOLDER)
    if abs_dags not in sys.path:
        sys.path.insert(0, abs_dags)
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


# ---------------------------------------------------------------------------
# All 8 DAG IDs
# ---------------------------------------------------------------------------
ALL_DAG_IDS = [
    "bronze_feature_releases",
    "bronze_user_signups",
    "bronze_feature_usage_events",
    "bronze_conversions",
    "silver_feature_states",
    "silver_user_dim",
    "silver_feature_usage_facts",
    "gold_feature_conversion_impact",
]


class TestDagLoading:
    """Verify all DAGs load without errors."""

    def test_no_import_errors(self, dagbag):
        assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"

    @pytest.mark.parametrize("dag_id", ALL_DAG_IDS)
    def test_dag_loaded(self, dagbag, dag_id):
        assert (
            dag_id in dagbag.dags
        ), f"DAG '{dag_id}' not found. Available: {list(dagbag.dags.keys())}"

    def test_dag_count(self, dagbag):
        """Exactly 8 DAGs should be loaded (no leftover monolithic DAG)."""
        assert len(dagbag.dags) == 8


class TestTaskStructure:
    """Every DAG must have exactly 3 tasks with the correct dependency chain."""

    @pytest.mark.parametrize("dag_id", ALL_DAG_IDS)
    def test_task_count(self, dagbag, dag_id):
        dag = dagbag.dags[dag_id]
        assert len(dag.tasks) == 3, f"DAG '{dag_id}' has {len(dag.tasks)} tasks, expected 3"

    @pytest.mark.parametrize("dag_id", ALL_DAG_IDS)
    def test_task_ids(self, dagbag, dag_id):
        dag = dagbag.dags[dag_id]
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {"assert_input_quality", "process", "assert_output_quality"}

    @pytest.mark.parametrize("dag_id", ALL_DAG_IDS)
    def test_dependency_chain(self, dagbag, dag_id):
        """assert_input_quality -> process -> assert_output_quality"""
        dag = dagbag.dags[dag_id]

        input_task = dag.get_task("assert_input_quality")
        process_task = dag.get_task("process")
        output_task = dag.get_task("assert_output_quality")

        # assert_input_quality has no upstream
        assert len(input_task.upstream_list) == 0

        # process depends on assert_input_quality
        assert {t.task_id for t in process_task.upstream_list} == {"assert_input_quality"}

        # assert_output_quality depends on process
        assert {t.task_id for t in output_task.upstream_list} == {"process"}

        # assert_output_quality has no downstream
        assert len(output_task.downstream_list) == 0

    @pytest.mark.parametrize("dag_id", ALL_DAG_IDS)
    def test_all_tasks_use_spark_submit(self, dagbag, dag_id):
        dag = dagbag.dags[dag_id]
        for task in dag.tasks:
            assert isinstance(
                task, SparkSubmitOperator
            ), f"Task '{task.task_id}' in DAG '{dag_id}' is not SparkSubmitOperator"


class TestDagConfiguration:
    """Verify DAG-level configuration."""

    @pytest.mark.parametrize("dag_id", ALL_DAG_IDS)
    def test_catchup_disabled(self, dagbag, dag_id):
        dag = dagbag.dags[dag_id]
        assert dag.catchup is False

    @pytest.mark.parametrize("dag_id", ALL_DAG_IDS)
    def test_default_args_owner(self, dagbag, dag_id):
        dag = dagbag.dags[dag_id]
        assert dag.default_args["owner"] == "data-engineering"

    @pytest.mark.parametrize("dag_id", ALL_DAG_IDS)
    def test_default_args_retries(self, dagbag, dag_id):
        dag = dagbag.dags[dag_id]
        assert dag.default_args["retries"] == 1


class TestBronzeSchedule:
    """Bronze DAGs should be scheduled @daily."""

    BRONZE_DAGS = [
        "bronze_feature_releases",
        "bronze_user_signups",
        "bronze_feature_usage_events",
        "bronze_conversions",
    ]

    @pytest.mark.parametrize("dag_id", BRONZE_DAGS)
    def test_daily_schedule(self, dagbag, dag_id):
        dag = dagbag.dags[dag_id]
        timetable_str = str(dag.timetable)
        assert "@daily" in timetable_str or dag.schedule == "@daily"


class TestDatasetDependencies:
    """Verify cross-DAG Dataset scheduling for silver and gold DAGs."""

    def _get_schedule_datasets(self, dag):
        """Extract dataset URIs from a DAG's schedule."""
        schedule = dag.schedule
        if hasattr(schedule, "datasets"):
            # DatasetOrTimeSchedule or DatasetSchedule
            return {ds.uri for ds in schedule.datasets}
        if isinstance(schedule, list):
            return {ds.uri for ds in schedule}
        # Try iterating (Airflow 2.9+ may use DatasetAll)
        try:
            return {ds.uri for ds in schedule}
        except TypeError:
            return set()

    def test_silver_feature_states_depends_on_bronze_feature_releases(self, dagbag):
        dag = dagbag.dags["silver_feature_states"]
        uris = self._get_schedule_datasets(dag)
        assert "delta://bronze/feature_releases" in uris

    def test_silver_user_dim_depends_on_bronze_signups_and_conversions(self, dagbag):
        dag = dagbag.dags["silver_user_dim"]
        uris = self._get_schedule_datasets(dag)
        assert "delta://bronze/user_signups" in uris
        assert "delta://bronze/conversions" in uris

    def test_silver_feature_usage_facts_depends_on_bronze_events(self, dagbag):
        dag = dagbag.dags["silver_feature_usage_facts"]
        uris = self._get_schedule_datasets(dag)
        assert "delta://bronze/feature_usage_events" in uris

    def test_gold_depends_on_all_upstream(self, dagbag):
        dag = dagbag.dags["gold_feature_conversion_impact"]
        uris = self._get_schedule_datasets(dag)
        assert "delta://silver/user_dim" in uris
        assert "delta://silver/feature_states" in uris
        assert "delta://silver/feature_usage_facts" in uris
        assert "delta://bronze/conversions" in uris


class TestProcessOutlets:
    """Verify that the 'process' task in each DAG has the correct outlet datasets."""

    def _get_outlet_uris(self, task):
        """Extract outlet dataset URIs from a task."""
        outlets = getattr(task, "outlets", [])
        return {ds.uri for ds in outlets}

    EXPECTED_OUTLETS = {
        "bronze_feature_releases": "delta://bronze/feature_releases",
        "bronze_user_signups": "delta://bronze/user_signups",
        "bronze_feature_usage_events": "delta://bronze/feature_usage_events",
        "bronze_conversions": "delta://bronze/conversions",
        "silver_feature_states": "delta://silver/feature_states",
        "silver_user_dim": "delta://silver/user_dim",
        "silver_feature_usage_facts": "delta://silver/feature_usage_facts",
        "gold_feature_conversion_impact": "delta://gold/feature_conversion_impact",
    }

    @pytest.mark.parametrize("dag_id", ALL_DAG_IDS)
    def test_process_outlet(self, dagbag, dag_id):
        dag = dagbag.dags[dag_id]
        process_task = dag.get_task("process")
        outlet_uris = self._get_outlet_uris(process_task)
        expected_uri = self.EXPECTED_OUTLETS[dag_id]
        assert expected_uri in outlet_uris, (
            f"DAG '{dag_id}' process task missing outlet '{expected_uri}'. " f"Got: {outlet_uris}"
        )


class TestValidationApplicationArgs:
    """Verify validation tasks pass --mode input/output correctly."""

    @pytest.mark.parametrize("dag_id", ALL_DAG_IDS)
    def test_input_validation_args(self, dagbag, dag_id):
        dag = dagbag.dags[dag_id]
        task = dag.get_task("assert_input_quality")
        args = getattr(task, "_application_args", None) or getattr(task, "application_args", None)
        assert args is not None
        assert "--mode" in args
        assert "input" in args

    @pytest.mark.parametrize("dag_id", ALL_DAG_IDS)
    def test_output_validation_args(self, dagbag, dag_id):
        dag = dagbag.dags[dag_id]
        task = dag.get_task("assert_output_quality")
        args = getattr(task, "_application_args", None) or getattr(task, "application_args", None)
        assert args is not None
        assert "--mode" in args
        assert "output" in args
