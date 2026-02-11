from pyspark.sql.functions import current_timestamp

from spark.jobs.bronze.user_signups.schema import USER_SIGNUPS_SCHEMA


def ingest_user_signups(spark, input_path, output_path):
    """
    Read user_signups JSONL (partitioned by date) -> Delta

    Schema: user_id, email, signup_date, company_size, industry, ingestion_timestamp

    Args:
        spark: SparkSession
        input_path: Path to user_signups.jsonl
        output_path: Path to bronze Delta table
    """
    print(f"Ingesting user_signups from {input_path} to {output_path}")

    # Read JSONL file
    df = spark.read.schema(USER_SIGNUPS_SCHEMA).json(input_path)

    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    # Write to Delta in append mode, partitioned by signup_date
    df.write.format("delta").mode("append").partitionBy("signup_date").save(output_path)

    row_count = df.count()
    print(f"Ingested {row_count} user signups to {output_path}")

    return row_count
