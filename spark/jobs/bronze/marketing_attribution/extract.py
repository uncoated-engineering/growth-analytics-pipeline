from pyspark.sql.functions import current_timestamp

from spark.jobs.bronze.marketing_attribution.schema import MARKETING_ATTRIBUTION_SCHEMA


def ingest_marketing_attribution(spark, input_path, output_path):
    """
    Read marketing_attribution JSONL -> Delta

    First-touch attribution: one record per tracked signup. Roughly 5% of
    users have no record (untracked traffic) — downstream joins are LEFT
    joins that default to 'unattributed'.

    Args:
        spark: SparkSession
        input_path: Path to marketing_attribution.jsonl
        output_path: Path to bronze Delta table
    """
    print(f"Ingesting marketing_attribution from {input_path} to {output_path}")

    df = spark.read.schema(MARKETING_ATTRIBUTION_SCHEMA).json(input_path)

    df = df.withColumn("ingestion_timestamp", current_timestamp())

    df.write.format("delta").mode("append").save(output_path)

    row_count = df.count()
    print(f"Ingested {row_count} attribution records to {output_path}")

    return row_count
