"""Governance test: declared lineage must match the Airflow Dataset wiring.

The YAML contracts in contracts/ declare each table's upstreams; the DAGs
declare their triggers (schedule datasets) and outputs (process outlets).
If they drift apart, the lineage docs and the chatbot grounding would lie
about how data actually flows — so this test fails instead.
"""

import os
import sys

import pytest
from airflow.models import DagBag

from semantic.catalog import Catalog

DAGS_FOLDER = os.path.join(os.path.dirname(__file__), "..", "..", "..", "airflow", "dags")


@pytest.fixture(scope="module")
def dagbag():
    abs_dags = os.path.abspath(DAGS_FOLDER)
    if abs_dags not in sys.path:
        sys.path.insert(0, abs_dags)
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


@pytest.fixture(scope="module")
def catalog():
    return Catalog()


def dataset_uri(catalog: Catalog, table_name: str) -> str:
    """Map a contract table to its Airflow Dataset URI.

    Bronze tables keep their name; silver/gold tables drop the layer prefix
    (silver_user_dim -> delta://silver/user_dim).
    """
    contract = catalog.tables[table_name]
    short_name = table_name.removeprefix(f"{contract.layer}_")
    return f"delta://{contract.layer}/{short_name}"


def schedule_uris(dag) -> set[str]:
    """Extract dataset URIs from a DAG's schedule (mirrors test_dags.py)."""
    schedule = dag.schedule
    if hasattr(schedule, "datasets"):
        return {ds.uri for ds in schedule.datasets}
    if isinstance(schedule, list):
        return {ds.uri for ds in schedule}
    try:
        return {ds.uri for ds in schedule}
    except TypeError:
        return set()


def outlet_uris(dag) -> set[str]:
    process = dag.get_task("process")
    return {o.uri for o in process.outlets}


class TestLineageMatchesAirflow:
    """Contracts and DAG wiring describe the same graph."""

    def test_every_table_has_a_dag(self, dagbag, catalog):
        """Each cataloged table is produced by exactly one DAG."""
        for table_name, contract in catalog.tables.items():
            dag_id = (
                table_name
                if table_name.startswith(contract.layer)
                else f"{contract.layer}_{table_name}"
            )
            assert dag_id in dagbag.dags, f"No DAG '{dag_id}' for table '{table_name}'"

    def test_dag_outlet_matches_contract_table(self, dagbag, catalog):
        """Each DAG's process task emits the Dataset of the table it builds."""
        for table_name, contract in catalog.tables.items():
            dag_id = (
                table_name
                if table_name.startswith(contract.layer)
                else f"{contract.layer}_{table_name}"
            )
            expected = {dataset_uri(catalog, table_name)}
            assert (
                outlet_uris(dagbag.dags[dag_id]) == expected
            ), f"DAG '{dag_id}' outlets don't match contract table"

    def test_dag_triggers_match_contract_upstreams(self, dagbag, catalog):
        """Silver/gold DAG schedules are exactly the contract upstream datasets."""
        for layer in ("silver", "gold"):
            for contract in catalog.by_layer(layer):
                dag_id = (
                    contract.table
                    if contract.table.startswith(layer)
                    else f"{layer}_{contract.table}"
                )
                expected = {dataset_uri(catalog, u) for u in contract.upstreams}
                actual = schedule_uris(dagbag.dags[dag_id])
                assert actual == expected, (
                    f"DAG '{dag_id}' triggers {sorted(actual)} but the contract "
                    f"declares upstreams {sorted(expected)}"
                )
