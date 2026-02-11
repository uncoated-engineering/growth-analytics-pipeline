from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Sentinel date for "currently active" SCD records
END_OF_TIME = date(9999, 12, 31)

FEATURE_STATES_SCHEMA = StructType(
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


def _get_empty_feature_states(spark: SparkSession):
    """Return an empty DataFrame with the silver_feature_states schema."""
    return spark.createDataFrame([], FEATURE_STATES_SCHEMA)
