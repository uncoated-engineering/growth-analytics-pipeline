"""
Silver Layer - SCD Type 2 Transformations

This module transforms bronze layer data into silver layer tables:
- silver_feature_states: SCD Type 2 dimension tracking feature version history
- silver_user_dim: Simple user dimension table
- silver_feature_usage_facts: Aggregated feature usage fact table

Uses Delta Lake MERGE operations for atomic upserts and change detection
via record hashing.
"""

from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    current_timestamp,
    datediff,
    lit,
    md5,
    row_number,
    to_date,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

# Sentinel date for "currently active" SCD records
END_OF_TIME = date(9999, 12, 31)


def _compute_record_hash(df: DataFrame) -> DataFrame:
    """Add a record_hash column based on feature_name + version for change detection."""
    from pyspark.sql.functions import concat_ws

    return df.withColumn("record_hash", md5(concat_ws("|", col("feature_name"), col("version"))))


def _get_empty_feature_states(spark: SparkSession) -> DataFrame:
    """Return an empty DataFrame with the silver_feature_states schema."""
    schema = StructType(
        [
            StructField("feature_id", IntegerType(), False),
            StructField("feature_name", StringType(), False),
            StructField("version", StringType(), False),
            StructField("is_enabled", BooleanType(), False),
            StructField("effective_from", DateType(), False),
            StructField("effective_to", DateType(), False),
            StructField("is_current", BooleanType(), False),
            StructField("record_hash", StringType(), False),
        ]
    )
    return spark.createDataFrame([], schema)


def maintain_feature_states_scd(spark: SparkSession, bronze_path: str, silver_path: str) -> int:
    """
    Maintain SCD Type 2 feature states from bronze feature releases.

    Input: bronze feature_releases (new releases/updates)
    Output: silver_feature_states (SCD Type 2 table)

    Schema:
        feature_id (INT)
        feature_name (STRING)
        version (STRING)
        is_enabled (BOOLEAN)
        effective_from (DATE)
        effective_to (DATE)      - 9999-12-31 for current records
        is_current (BOOLEAN)     - True for active record
        record_hash (STRING)     - md5(feature_name|version) for change detection

    Args:
        spark: SparkSession
        bronze_path: Base path for bronze Delta tables
        silver_path: Base path for silver Delta tables

    Returns:
        Number of rows in the resulting silver table
    """
    output_path = f"{silver_path}/silver_feature_states"
    print(f"Maintaining SCD Type 2 feature states at {output_path}")

    # Step 1: Read new feature releases from bronze
    new_releases = spark.read.format("delta").load(f"{bronze_path}/feature_releases")

    # Prepare incoming records with SCD fields
    incoming = (
        new_releases.select("feature_id", "feature_name", "version", "release_date")
        .withColumn("is_enabled", lit(True))
        .withColumn("effective_from", to_date(col("release_date")))
        .withColumn("effective_to", lit(END_OF_TIME).cast(DateType()))
        .withColumn("is_current", lit(True))
        .drop("release_date")
    )
    incoming = _compute_record_hash(incoming)

    # Step 2: Check if silver table already exists
    try:
        existing = spark.read.format("delta").load(output_path)
        table_exists = True
    except Exception:
        table_exists = False

    if not table_exists:
        # First run: write all incoming as new SCD records
        incoming.write.format("delta").mode("overwrite").save(output_path)
        result_df = spark.read.format("delta").load(output_path)
        row_count = result_df.count()
        print(f"  Initial load: {row_count} feature state records created")
        return row_count

    # Step 3: Detect changes - find records where feature_id matches but hash differs
    existing_current = existing.filter(col("is_current") == True)  # noqa: E712

    # Join incoming with existing current records to find changes
    changes = (
        incoming.alias("new")
        .join(
            existing_current.alias("old"),
            col("new.feature_id") == col("old.feature_id"),
            "inner",
        )
        .filter(col("new.record_hash") != col("old.record_hash"))
    )

    # Find truly new features (no existing record at all)
    new_features = incoming.alias("new").join(
        existing.alias("old"),
        col("new.feature_id") == col("old.feature_id"),
        "left_anti",
    )

    has_changes = changes.count() > 0
    has_new = new_features.count() > 0

    if not has_changes and not has_new:
        row_count = existing.count()
        print(f"  No changes detected: {row_count} total feature state records")
        return row_count

    # Build staged updates for a single atomic MERGE operation.
    # Uses merge_key pattern: real feature_id for updates, -1 for inserts.
    staged_parts = []

    if has_changes:
        # Rows that will MATCH existing records and close them
        staged_parts.append(
            changes.select(
                col("new.feature_id").alias("merge_key"),
                col("old.feature_id"),
                col("old.feature_name"),
                col("old.version"),
                col("old.is_enabled"),
                col("old.effective_from"),
                col("new.effective_from").alias("effective_to"),
                lit(False).alias("is_current"),
                col("old.record_hash"),
            )
        )
        # Rows that will NOT match (merge_key=-1) and get inserted as new versions
        staged_parts.append(
            changes.select(
                lit(-1).alias("merge_key"),
                col("new.feature_id"),
                col("new.feature_name"),
                col("new.version"),
                col("new.is_enabled"),
                col("new.effective_from"),
                col("new.effective_to"),
                col("new.is_current"),
                col("new.record_hash"),
            )
        )

    if has_new:
        # Completely new features (merge_key=-1, will be inserted)
        staged_parts.append(
            new_features.select(
                lit(-1).alias("merge_key"),
                col("feature_id"),
                col("feature_name"),
                col("version"),
                col("is_enabled"),
                col("effective_from"),
                col("effective_to"),
                col("is_current"),
                col("record_hash"),
            )
        )

    # Union all staged rows
    staged = staged_parts[0]
    for part in staged_parts[1:]:
        staged = staged.union(part)

    # Step 4 + 5: Single atomic MERGE - close old records AND insert new ones
    delta_table = DeltaTable.forPath(spark, output_path)
    delta_table.alias("target").merge(
        staged.alias("source"),
        "target.feature_id = source.merge_key AND target.is_current = true",
    ).whenMatchedUpdate(
        set={
            "effective_to": col("source.effective_to"),
            "is_current": lit(False),
        }
    ).whenNotMatchedInsert(
        values={
            "feature_id": col("source.feature_id"),
            "feature_name": col("source.feature_name"),
            "version": col("source.version"),
            "is_enabled": col("source.is_enabled"),
            "effective_from": col("source.effective_from"),
            "effective_to": col("source.effective_to"),
            "is_current": col("source.is_current"),
            "record_hash": col("source.record_hash"),
        }
    ).execute()

    result_df = spark.read.format("delta").load(output_path)
    row_count = result_df.count()
    print(f"  SCD Type 2 complete: {row_count} total feature state records")
    return row_count


def maintain_user_dim(spark: SparkSession, bronze_path: str, silver_path: str) -> int:
    """
    Create/update a simple user dimension table from bronze user signups.

    This is a simplified dimension (not full SCD Type 2 for MVP).
    Later could add SCD for plan changes (free -> pro -> enterprise).

    Schema:
        user_id (INT)
        signup_date (DATE)
        company_size (STRING)
        industry (STRING)
        current_plan (STRING)    - Joined from conversions, defaults to 'free'

    Args:
        spark: SparkSession
        bronze_path: Base path for bronze Delta tables
        silver_path: Base path for silver Delta tables

    Returns:
        Number of rows in the resulting silver table
    """
    output_path = f"{silver_path}/silver_user_dim"
    print(f"Maintaining user dimension at {output_path}")

    # Read bronze user signups
    signups = spark.read.format("delta").load(f"{bronze_path}/user_signups")

    # Read bronze conversions to get current plan
    conversions = spark.read.format("delta").load(f"{bronze_path}/conversions")

    # Get the latest conversion per user using window function (avoids self-join ambiguity)
    conv_window = Window.partitionBy("user_id").orderBy(col("conversion_date").desc())
    latest_conversions = (
        conversions.withColumn("rn", row_number().over(conv_window))
        .filter(col("rn") == 1)
        .select(
            col("user_id").alias("conv_user_id"),
            col("plan").alias("current_plan"),
        )
    )

    # Build user dimension: join signups with latest conversion
    user_dim = (
        signups.select("user_id", "signup_date", "company_size", "industry")
        .dropDuplicates(["user_id"])
        .join(latest_conversions, col("user_id") == col("conv_user_id"), "left")
        .withColumn("current_plan", coalesce(col("current_plan"), lit("free")))
        .withColumn("signup_date", to_date(col("signup_date")))
        .select("user_id", "signup_date", "company_size", "industry", "current_plan")
    )

    # Write/overwrite the dimension table
    user_dim.write.format("delta").mode("overwrite").save(output_path)

    row_count = user_dim.count()
    print(f"  User dimension complete: {row_count} users")
    return row_count


def create_feature_usage_facts(spark: SparkSession, bronze_path: str, silver_path: str) -> int:
    """
    Aggregate bronze feature usage events into a silver fact table.

    Creates per-user, per-feature usage summaries that enable time-point queries
    like "What did usage look like on 2024-06-01?"

    Schema:
        user_id (INT)
        feature_id (INT)
        first_used_date (DATE)      - Important for "used before conversion" analysis
        last_used_date (DATE)
        total_usage_count (INT)
        avg_daily_usage (DOUBLE)
        as_of_date (DATE)           - Snapshot date

    Args:
        spark: SparkSession
        bronze_path: Base path for bronze Delta tables
        silver_path: Base path for silver Delta tables

    Returns:
        Number of rows in the resulting silver table
    """
    output_path = f"{silver_path}/silver_feature_usage_facts"
    print(f"Creating feature usage facts at {output_path}")

    # Read bronze feature usage events
    events = spark.read.format("delta").load(f"{bronze_path}/feature_usage_events")

    # Aggregate per user + feature
    usage_facts = events.groupBy("user_id", "feature_id").agg(
        spark_min("event_date").alias("first_used_date"),
        spark_max("event_date").alias("last_used_date"),
        count("*").alias("total_usage_count"),
    )

    # Calculate avg_daily_usage: total_usage_count / (last_used_date - first_used_date + 1)
    usage_facts = usage_facts.withColumn(
        "date_span",
        datediff(col("last_used_date"), col("first_used_date")) + 1,
    )
    usage_facts = usage_facts.withColumn(
        "avg_daily_usage",
        (col("total_usage_count") / col("date_span")).cast(DoubleType()),
    ).drop("date_span")

    # Add snapshot date (as_of_date) = current date
    usage_facts = usage_facts.withColumn("as_of_date", current_timestamp().cast(DateType()))

    # Select final column order
    usage_facts = usage_facts.select(
        "user_id",
        "feature_id",
        "first_used_date",
        "last_used_date",
        "total_usage_count",
        "avg_daily_usage",
        "as_of_date",
    )

    # Write/overwrite the fact table
    usage_facts.write.format("delta").mode("overwrite").save(output_path)

    row_count = usage_facts.count()
    print(f"  Feature usage facts complete: {row_count} user-feature combinations")
    return row_count


def run_silver_transformation(
    spark: SparkSession,
    bronze_path: str = "data/bronze",
    silver_path: str = "data/silver",
) -> dict:
    """
    Run all silver layer transformation jobs.

    Args:
        spark: SparkSession
        bronze_path: Base path for bronze Delta tables
        silver_path: Base path for silver Delta tables

    Returns:
        Dictionary with row counts per silver table
    """
    print("=" * 80)
    print("Starting Silver Layer Transformation")
    print("=" * 80)

    transformation_stats = {}

    # SCD Type 2 feature states
    transformation_stats["feature_states"] = maintain_feature_states_scd(
        spark, bronze_path, silver_path
    )

    # User dimension
    transformation_stats["user_dim"] = maintain_user_dim(spark, bronze_path, silver_path)

    # Feature usage facts
    transformation_stats["feature_usage_facts"] = create_feature_usage_facts(
        spark, bronze_path, silver_path
    )

    print("=" * 80)
    print("Silver Layer Transformation Complete")
    print("=" * 80)
    print("\nTransformation Summary:")
    for table, row_count in transformation_stats.items():
        print(f"  {table}: {row_count} rows")

    return transformation_stats


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    # Create Spark session with Delta Lake support
    builder = (
        SparkSession.builder.appName("Silver Layer Transformation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "spark-warehouse")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    try:
        stats = run_silver_transformation(spark)

        # Validation: Query silver tables to verify
        print("\n" + "=" * 80)
        print("Validation: Verifying Silver Tables")
        print("=" * 80)

        for table_name in [
            "silver_feature_states",
            "silver_user_dim",
            "silver_feature_usage_facts",
        ]:
            table_path = f"data/silver/{table_name}"
            df = spark.read.format("delta").load(table_path)
            row_count = df.count()
            print(f"\n{table_name}:")
            print(f"  Row count: {row_count}")
            print("  Schema:")
            df.printSchema()
            print("  Sample data:")
            df.show(5, truncate=False)

        # SCD Type 2 validation query
        print("\n" + "=" * 80)
        print("SCD Type 2 Validation")
        print("=" * 80)
        feature_states = spark.read.format("delta").load("data/silver/silver_feature_states")
        print("\nAll current feature states:")
        feature_states.filter(col("is_current") == True).show(truncate=False)  # noqa: E712

    finally:
        spark.stop()
