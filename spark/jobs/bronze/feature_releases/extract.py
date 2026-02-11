from pyspark.sql.functions import current_timestamp

from spark.jobs.bronze.feature_releases.schema import FEATURE_RELEASES_SCHEMA


def ingest_feature_releases(spark, input_path, output_path):
    """
    Read feature_releases JSON -> Write to Delta (append mode)

    Schema: feature_id, feature_name, release_date, version, ingestion_timestamp

    Args:
        spark: SparkSession
        input_path: Path to feature_releases.json
        output_path: Path to bronze Delta table
    """
    print(f"Ingesting feature_releases from {input_path} to {output_path}")

    # Read JSON file (multiLine=True for standard JSON array format)
    df = spark.read.schema(FEATURE_RELEASES_SCHEMA).option("multiLine", True).json(input_path)

    # Rename 'id' to 'feature_id' and 'name' to 'feature_name' for clarity
    df = df.withColumnRenamed("id", "feature_id").withColumnRenamed("name", "feature_name")

    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    # Write to Delta in append mode
    df.write.format("delta").mode("append").save(output_path)

    row_count = df.count()
    print(f"Ingested {row_count} feature releases to {output_path}")

    return row_count
