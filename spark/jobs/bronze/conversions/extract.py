from pyspark.sql.functions import current_timestamp

from spark.jobs.bronze.conversions.schema import CONVERSIONS_SCHEMA


def ingest_conversions(spark, input_path, output_path):
    """
    Read conversions JSONL -> Delta

    Schema: user_id, conversion_date, plan, mrr, ingestion_timestamp

    Args:
        spark: SparkSession
        input_path: Path to conversions.jsonl
        output_path: Path to bronze Delta table
    """
    print(f"Ingesting conversions from {input_path} to {output_path}")

    # Read JSONL file
    df = spark.read.schema(CONVERSIONS_SCHEMA).json(input_path)

    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    # Write to Delta in append mode
    df.write.format("delta").mode("append").save(output_path)

    row_count = df.count()
    print(f"Ingested {row_count} conversions to {output_path}")

    return row_count
