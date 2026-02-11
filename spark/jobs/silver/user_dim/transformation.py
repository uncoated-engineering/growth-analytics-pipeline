from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit, row_number, to_date
from pyspark.sql.window import Window


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
