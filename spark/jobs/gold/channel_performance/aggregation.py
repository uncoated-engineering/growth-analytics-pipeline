from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, coalesce, col, count, lit, row_number, trunc, when
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql.window import Window

from spark.jobs.gold.channel_performance.schema import CHANNEL_PERFORMANCE_COLUMNS


def calculate_channel_performance(
    spark: SparkSession, bronze_path: str, silver_path: str, gold_path: str
) -> int:
    """
    Measure acquisition funnel quality per channel per signup-month cohort.

    Joins the user dimension (which carries first-touch attribution) with
    conversions to answer: which channels bring users who actually convert,
    how fast, and at what revenue?

    Args:
        spark: SparkSession
        bronze_path: Base path for bronze Delta tables
        silver_path: Base path for silver Delta tables
        gold_path: Base path for gold Delta tables

    Returns:
        Number of rows in the resulting gold table
    """
    output_path = f"{gold_path}/gold_channel_performance"
    print(f"Calculating channel performance at {output_path}")

    users = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
    conversions = spark.read.format("delta").load(f"{bronze_path}/conversions")

    # Latest conversion per user (defensive dedup, mirrors other gold jobs)
    conv_window = Window.partitionBy("user_id").orderBy(col("conversion_date").desc())
    latest_conversions = (
        conversions.withColumn("rn", row_number().over(conv_window))
        .filter(col("rn") == 1)
        .select(
            col("user_id").alias("conv_user_id"),
            col("mrr"),
            col("days_to_convert"),
        )
    )

    enriched = users.join(
        latest_conversions, col("user_id") == col("conv_user_id"), "left"
    ).withColumn("signup_month", trunc(col("signup_date"), "month"))

    result_df = (
        enriched.groupBy("signup_month", "acquisition_channel")
        .agg(
            count("*").cast(LongType()).alias("signups"),
            spark_sum(when(col("conv_user_id").isNotNull(), 1).otherwise(0))
            .cast(LongType())
            .alias("conversions"),
            avg(when(col("conv_user_id").isNotNull(), 1.0).otherwise(0.0)).alias("conversion_rate"),
            avg(col("days_to_convert")).alias("avg_days_to_convert"),
            coalesce(spark_sum(col("mrr")), lit(0)).cast(LongType()).alias("total_new_mrr"),
            avg(col("mrr")).cast(DoubleType()).alias("avg_new_mrr"),
        )
        .select(*CHANNEL_PERFORMANCE_COLUMNS)
        .orderBy("signup_month", "acquisition_channel")
    )

    result_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        output_path
    )

    row_count = result_df.count()
    print(f"  Channel performance complete: {row_count} month-channel rows")
    return row_count
