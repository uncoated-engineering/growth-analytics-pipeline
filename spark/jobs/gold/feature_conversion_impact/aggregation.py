from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    current_date,
    datediff,
    lit,
    row_number,
    when,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.window import Window


def calculate_feature_conversion_impact(
    spark: SparkSession, bronze_path: str, silver_path: str, gold_path: str
) -> int:
    """
    Calculate feature impact on conversion rates using cohort analysis.

    Joins users with features and their usage to determine whether using
    a feature before conversion correlates with higher conversion rates.

    Output schema:
        feature_name (STRING)
        cohort (STRING)             - 'used_before_conversion', 'available_not_used',
                                      or 'not_available'
        total_users (LONG)
        converted_users (LONG)
        conversion_rate (DOUBLE)
        avg_days_to_convert (DOUBLE)
        avg_mrr (DOUBLE)

    Args:
        spark: SparkSession
        bronze_path: Base path for bronze Delta tables
        silver_path: Base path for silver Delta tables
        gold_path: Base path for gold Delta tables

    Returns:
        Number of rows in the resulting gold table
    """
    output_path = f"{gold_path}/gold_feature_conversion_impact"
    print(f"Calculating feature conversion impact at {output_path}")

    # Read silver tables
    users = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
    feature_states = spark.read.format("delta").load(f"{silver_path}/silver_feature_states")
    usage_facts = spark.read.format("delta").load(f"{silver_path}/silver_feature_usage_facts")

    # Read bronze conversions for conversion_date and mrr
    conversions = spark.read.format("delta").load(f"{bronze_path}/conversions")

    # Get latest conversion per user (in case of duplicates)
    conv_window = Window.partitionBy("user_id").orderBy(col("conversion_date").desc())
    latest_conversions = (
        conversions.withColumn("rn", row_number().over(conv_window))
        .filter(col("rn") == 1)
        .select(
            col("user_id").alias("conv_user_id"),
            col("conversion_date"),
            col("mrr"),
        )
    )

    # Get only current feature states
    current_features = feature_states.filter(col("is_current") == True).select(  # noqa: E712
        "feature_id", "feature_name", "effective_from", "effective_to"
    )

    # Step 1: Build user_feature_context
    # Users LEFT JOIN conversions -> gives us conversion_date and mrr per user
    users_with_conv = users.join(
        latest_conversions,
        col("user_id") == col("conv_user_id"),
        "left",
    ).drop("conv_user_id")

    # CROSS JOIN with current features -> one row per user per feature
    user_feature = users_with_conv.crossJoin(current_features)

    # LEFT JOIN with usage facts -> did this user use this feature?
    user_feature_context = user_feature.join(
        usage_facts.select(
            col("user_id").alias("ufu_user_id"),
            col("feature_id").alias("ufu_feature_id"),
            col("first_used_date"),
        ),
        (col("user_id") == col("ufu_user_id")) & (col("feature_id") == col("ufu_feature_id")),
        "left",
    ).drop("ufu_user_id", "ufu_feature_id")

    # Compute feature_available flag:
    # Was the feature available during the user's trial period?
    reference_date = coalesce(col("conversion_date"), current_date())
    user_feature_context = user_feature_context.withColumn(
        "feature_available",
        (col("effective_from") <= reference_date) & (col("effective_to") > reference_date),
    )

    # Compute used_before_conversion flag:
    # Did the user try the feature before converting?
    user_feature_context = user_feature_context.withColumn(
        "used_before_conversion",
        col("first_used_date").isNotNull()
        & col("conversion_date").isNotNull()
        & (col("first_used_date") < col("conversion_date")),
    )

    # Step 2: Assign cohorts
    user_feature_context = user_feature_context.withColumn(
        "cohort",
        when(col("used_before_conversion"), lit("used_before_conversion"))
        .when(
            col("feature_available") & ~col("used_before_conversion"),
            lit("available_not_used"),
        )
        .otherwise(lit("not_available")),
    )

    # Step 3: Aggregate by feature_name + cohort
    result_df = (
        user_feature_context.groupBy("feature_name", "cohort")
        .agg(
            count("*").alias("total_users"),
            spark_sum(when(col("conversion_date").isNotNull(), 1).otherwise(0)).alias(
                "converted_users"
            ),
            avg(when(col("conversion_date").isNotNull(), 1.0).otherwise(0.0)).alias(
                "conversion_rate"
            ),
            avg(
                when(
                    col("conversion_date").isNotNull(),
                    datediff(col("conversion_date"), col("signup_date")),
                )
            ).alias("avg_days_to_convert"),
            avg(when(col("conversion_date").isNotNull(), col("mrr"))).alias("avg_mrr"),
        )
        .orderBy(col("conversion_rate").desc())
    )

    # Select final column order
    result_df = result_df.select(
        "feature_name",
        "cohort",
        "total_users",
        "converted_users",
        "conversion_rate",
        "avg_days_to_convert",
        "avg_mrr",
    )

    # Write to Gold Delta table
    result_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        output_path
    )

    row_count = result_df.count()
    print(f"  Feature conversion impact complete: {row_count} cohort rows")
    return row_count
