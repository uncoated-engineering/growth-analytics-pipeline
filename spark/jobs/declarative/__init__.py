"""
Declarative pipeline framework for the growth analytics pipeline.

This package provides config-driven pipeline execution using a YAML manifest
as the single source of truth. Instead of writing imperative code for each
table, declare schemas, transformations, quality rules, and dependencies
in pipeline.yaml and let the engines handle execution.

Components:
    manifest     - Load and validate the pipeline manifest
    schema_registry - Convert YAML schemas to PySpark StructTypes
    quality_engine  - Declarative data quality rules execution
    bronze_engine   - Config-driven bronze ingestion
    lineage         - Pipeline lineage graph and impact analysis
"""
