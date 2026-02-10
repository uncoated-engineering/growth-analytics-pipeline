"""
Pytest configuration and shared fixtures for Spark tests.
"""

import shutil
import tempfile

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for testing with Delta Lake support.
    Session-scoped to reuse across all tests for performance.
    """
    builder = (
        SparkSession.builder.appName("pytest-spark")
        .master("local[2]")  # Use 2 cores for parallel processing
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.ui.enabled", "false")  # Disable UI for tests
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")  # Force driver to use localhost
        .config("spark.sql.shuffle.partitions", "1")  # Single partition for tests
    )

    # Configure Delta Lake support
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Reduce logging noise during tests
    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    # Cleanup
    spark.stop()


@pytest.fixture(scope="function")
def temp_dir():
    """
    Create a temporary directory for test data.
    Function-scoped so each test gets a clean directory.
    """
    temp_path = tempfile.mkdtemp()
    yield temp_path
    # Cleanup after test
    shutil.rmtree(temp_path, ignore_errors=True)
