# Growth Analytics Pipeline

A SaaS Product-Led Growth (PLG) analytics pipeline built with Apache Spark, Delta Lake, and Apache Airflow. This project demonstrates a modern data lakehouse architecture for analyzing user behavior, feature adoption, and conversion patterns.

## Architecture

The pipeline follows a medallion architecture with three layers:

- **Bronze Layer**: Raw data ingestion from JSON/JSONL to Delta Lake
- **Silver Layer**: Data transformation with SCD Type 2 (Slowly Changing Dimensions)
- **Gold Layer**: Business-level aggregations and analytics (cohort analysis)

## Project Structure

```
growth-analytics-pipeline/
├── data/
│   ├── raw/                    # Raw JSON/JSONL source data
│   ├── bronze/                 # Bronze Delta tables (raw ingestion)
│   ├── silver/                 # Silver Delta tables (cleaned & transformed)
│   └── gold/                   # Gold Delta tables (aggregated analytics)
├── spark/
│   ├── jobs/                   # Spark job implementations
│   │   ├── bronze_ingestion.py      # Bronze layer: raw to Delta
│   │   ├── silver_scd_transform.py  # Silver layer: SCD Type 2
│   │   └── gold_cohort_analysis.py  # Gold layer: cohort analytics
│   └── tests/                  # Unit tests
├── airflow/
│   ├── dags/                   # Airflow DAG definitions
│   │   └── saas_plg_pipeline.py     # Main pipeline DAG
│   ├── plugins/                # Custom Airflow operators
│   └── tests/                  # DAG tests
│       └── test_dag.py
├── scripts/
│   └── generate_synthetic_data.py   # Generate test data
├── notebooks/                  # Jupyter notebooks for analysis
├── docker/                     # Docker compose for services
├── agent_instructions/         # Implementation guidelines
├── pyproject.toml             # Project dependencies (UV/pip)
└── Makefile                   # Common commands

```

## Getting Started

### Prerequisites

- Python 3.12+
- UV (recommended) or pip
- Java 11+ (for Spark)
- Docker & Docker Compose (optional, for Airflow)

### Installation

1. **Create Python environment and install dependencies:**

```bash
make setup
```

Or manually:

```bash
uv venv --python 3.12
source .venv/bin/activate
uv sync
```

2. **Install pre-commit hooks (recommended):**

```bash
make pre-commit-install
```

This sets up automatic code quality checks before each commit.

2. **Generate synthetic data:**

```bash
make generate-data
```

This creates sample data files in `data/raw/`:
- `feature_releases.json` - Product feature releases
- `user_signups.jsonl` - User registration data
- `feature_usage_events.jsonl` - Feature interaction events
- `conversions.jsonl` - User conversion events

## Pipeline Execution

### Bronze Layer - Raw Data Ingestion

The bronze layer ingests raw JSON/JSONL files into Delta Lake tables with minimal transformation.

**Run bronze ingestion:**

```bash
make ingest-bronze
```

**What it does:**

1. **Feature Releases**: Reads `feature_releases.json` (JSON array)
   - Schema: `feature_id, feature_name, release_date, version, ingestion_timestamp`
   - Output: `data/bronze/feature_releases/`

2. **User Signups**: Reads `user_signups.jsonl` (JSONL)
   - Schema: `user_id, email, signup_date, company_size, industry, ingestion_timestamp`
   - Partitioned by: `signup_date`
   - Output: `data/bronze/user_signups/`

3. **Feature Usage Events**: Reads `feature_usage_events.jsonl` (JSONL)
   - Schema: `event_id, user_id, feature_id, feature_name, event_type, event_timestamp, event_date, ingestion_timestamp`
   - Partitioned by: `event_date` (for query performance)
   - Output: `data/bronze/feature_usage_events/`

4. **Conversions**: Reads `conversions.jsonl` (JSONL)
   - Schema: `user_id, conversion_date, plan, mrr, signup_date, days_to_convert, used_real_time_collab, ingestion_timestamp`
   - Output: `data/bronze/conversions/`

**Features:**
- All tables use Delta Lake format for ACID transactions
- Automatic schema inference with type safety
- Partition pruning for large tables (events, signups)
- Idempotent writes in append mode
- Automatic `ingestion_timestamp` for data lineage

### Silver Layer - SCD Type 2 Transformation

The silver layer transforms bronze data into analytical-ready tables using Slowly Changing Dimensions (SCD) Type 2 for historical tracking.

**Run silver transformation:**

```bash
make ingest-silver
```

**What it does:**

1. **Feature States (SCD Type 2)**: Tracks feature version history with change detection
   - Schema: `feature_id, feature_name, version, is_enabled, effective_from, effective_to, is_current, record_hash`
   - Uses Delta MERGE with staged updates pattern for atomic upserts
   - `record_hash` (MD5 of feature_name + version) detects changes
   - `effective_to = 9999-12-31` marks current records; old versions get closed with the new version's effective date
   - Output: `data/silver/silver_feature_states/`

2. **User Dimension**: Simplified user dimension enriched with conversion data
   - Schema: `user_id, signup_date, company_size, industry, current_plan`
   - Joins signups with latest conversion; defaults to `'free'` for non-converted users
   - Output: `data/silver/silver_user_dim/`

3. **Feature Usage Facts**: Aggregated per-user, per-feature usage summaries
   - Schema: `user_id, feature_id, first_used_date, last_used_date, total_usage_count, avg_daily_usage, as_of_date`
   - Enables "used before conversion" analysis via `first_used_date`
   - `as_of_date` supports time-point snapshot queries
   - Output: `data/silver/silver_feature_usage_facts/`

**Features:**
- SCD Type 2 with Delta MERGE for atomic close-and-insert operations
- Idempotent: re-running with unchanged data produces no duplicates
- Time-travel queries: filter by `effective_from`/`effective_to` for point-in-time state
- Change detection via MD5 record hashing

### Gold Layer - Cohort Analysis

The gold layer builds business-level analytics from silver and bronze data, answering the key question: **"Does feature adoption drive conversion?"**

**Run gold aggregation:**

```bash
make ingest-gold
```

**What it does:**

1. **Feature Conversion Impact**: Cohort analysis correlating feature usage with conversion rates
   - Schema: `feature_name, cohort, total_users, converted_users, conversion_rate, avg_days_to_convert, avg_mrr`
   - Cohorts: `used_before_conversion`, `available_not_used`, `not_available`
   - Overwrite mode with schema evolution
   - Output: `data/gold/gold_feature_conversion_impact/`

**Key Insight:**
Compare conversion rates across cohorts to measure feature impact:
```
cohort                  | conversion_rate
-----------------------------------------
used_before_conversion  | 22% (feature adopters)
available_not_used      | 18% (non-adopters)
→ 22% / 18% = 1.22x = ~23% conversion lift
```

**Features:**
- Cross-joins users with all current features for complete cohort coverage
- Correctly handles non-converted users (counted in totals, excluded from averages)
- Delta Lake overwrite mode ensures idempotent re-runs

### Run Full Pipeline

```bash
make pipeline
```

This executes all three layers sequentially: bronze → silver → gold.

## Orchestration with Airflow

The pipeline is orchestrated by an Airflow DAG (`airflow/dags/saas_plg_pipeline.py`) that manages task dependencies and scheduling.

### DAG: `saas_plg_analytics_pipeline`

- **Schedule**: `@daily`
- **Owner**: `data-engineering`
- **Retries**: 1 (5-minute delay)
- **Catchup**: Disabled

### Task Dependency Graph

```
ingest_feature_releases ─┐
ingest_user_signups ─────┤
ingest_feature_usage ────┼──→ maintain_feature_states_scd ──→ build_feature_usage_facts ──→ calculate_feature_conversion_impact
ingest_conversions ──────┘
```

- **Bronze tasks** run in parallel (no inter-dependencies)
- **Silver tasks** run sequentially (SCD must complete before usage facts)
- **Gold task** runs after all silver tasks complete

### Tasks

| Task ID | Layer | Spark Job |
|---------|-------|-----------|
| `ingest_feature_releases` | Bronze | `bronze_ingestion.py` |
| `ingest_user_signups` | Bronze | `bronze_ingestion.py` |
| `ingest_feature_usage` | Bronze | `bronze_ingestion.py` |
| `ingest_conversions` | Bronze | `bronze_ingestion.py` |
| `maintain_feature_states_scd` | Silver | `silver_scd_transform.py` |
| `build_feature_usage_facts` | Silver | `silver_scd_transform.py` |
| `calculate_feature_conversion_impact` | Gold | `gold_cohort_analysis.py` |

### Running Airflow

```bash
# Initialize Airflow database
make airflow-init

# Start Airflow services via Docker
make docker-up

# Run DAG tests
make test-airflow
```

### Validation

1. Trigger DAG manually in Airflow UI
2. Verify all tasks turn green
3. Check task logs for errors

## Development

### Run Tests

```bash
# Run Spark job tests
make test

# Run Airflow DAG tests
make test-airflow
```

### Code Quality

The project includes code quality tools configured in `pyproject.toml`:

```bash
# Format code with black and ruff
make format

# Lint code with ruff
make lint

# Check type hints with mypy
make assert-typing
```

#### Pre-commit Hooks

Pre-commit hooks automatically run quality checks before each commit:

```bash
# Install hooks (one-time setup)
make pre-commit-install

# Run hooks manually on all files
make pre-commit-run

# Update hooks to latest versions
make pre-commit-update
```

The hooks will automatically:
- Format code with black
- Lint and fix issues with ruff
- Check type hints with mypy
- Remove trailing whitespace
- Fix end of files
- Validate YAML, JSON, and TOML syntax
- Check for large files and merge conflicts

### Jupyter Notebooks

```bash
make notebook
```

## Delta Lake Tables

### Querying Bronze Tables

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Query Bronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read feature releases
df = spark.read.format("delta").load("data/bronze/feature_releases")
df.show()

# Read with partition filtering (efficient!)
df = spark.read.format("delta").load("data/bronze/user_signups") \
    .filter("signup_date >= '2024-01-01'")
df.show()
```

### Verifying Ingestion

After running `make ingest-bronze`, verify tables:

```bash
# Check Delta table structure
ls -R data/bronze/

# Count records per table
spark-submit --packages io.delta:delta-spark_2.12:3.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -c "
spark.read.format('delta').load('data/bronze/feature_releases').count()
"
```

## Data Flow

```
Raw Data (JSON/JSONL)
    ↓                        ┌──────────────────────────┐
[Bronze Layer] ─ parallel ─→ │   Airflow DAG (@daily)   │
    ↓                        │   Orchestrates all tasks  │
[Silver Layer] ─ sequential  │   with SparkSubmitOperator│
    ↓                        └──────────────────────────┘
[Gold Layer] - Business metrics & cohort analysis
    ↓
Analytics & Reporting
```

## Technology Stack

- **Apache Spark 3.5+**: Distributed data processing
- **Delta Lake 3.0+**: ACID transactions, time travel, schema evolution
- **Apache Airflow 2.8+**: Workflow orchestration
- **Python 3.12**: Core language
- **Faker**: Synthetic data generation
- **Pandas/NumPy**: Data manipulation
- **Jupyter**: Interactive analysis
- **UV**: Fast Python package manager

## Makefile Commands

```bash
make help                 # Show all available commands
make setup                # Create venv and install dependencies
make generate-data        # Generate synthetic test data
make format               # Format code with black and ruff
make lint                 # Lint code with ruff
make assert-typing        # Check type hints with mypy
make pre-commit-install   # Install pre-commit hooks
make pre-commit-run       # Run pre-commit hooks on all files
make pre-commit-update    # Update pre-commit hooks
make ingest-bronze        # Run bronze layer ingestion
make ingest-silver        # Run silver layer transformation
make ingest-gold          # Run gold layer aggregation
make pipeline             # Run full pipeline (bronze → silver → gold)
make test                 # Run Spark job unit tests
make test-airflow         # Run Airflow DAG tests
make clean                # Remove cache and temp files
make notebook             # Start Jupyter notebook server
```

## Contributing

This is a learning/demonstration project. Feel free to:
- Add more synthetic data scenarios
- Implement additional analytics in the gold layer
- Add data quality checks
- Improve error handling
- Add more comprehensive tests

## License

MIT License

## Acknowledgments

Built as a demonstration of modern data engineering practices with Delta Lake and Spark.
