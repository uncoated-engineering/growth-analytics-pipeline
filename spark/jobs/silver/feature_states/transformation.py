from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, lit, md5, to_date
from pyspark.sql.types import DateType

from spark.jobs.silver.feature_states.schema import END_OF_TIME


def _compute_record_hash(df: DataFrame) -> DataFrame:
    """Add a record_hash column based on feature_name + version for change detection."""
    return df.withColumn("record_hash", md5(concat_ws("|", col("feature_name"), col("version"))))


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
