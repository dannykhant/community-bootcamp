from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType
from pyspark.sql.functions import col

from ..jobs.assignment2b import do_scd_generation
from collections import namedtuple
from datetime import datetime

actors = namedtuple(
    "Actors",
    "actor_id actor quality_class is_active current_year"
)

actors_scd = namedtuple(
    "ActorsScd",
    "actor_id actor quality_class is_active start_year end_year current_year"
)


def test_datelist(spark):
    input_data = [
        actors(1, "Tom Hank", "star", True, 1970),
        actors(1, "Tom Hank", "star", True, 1971),
        actors(1, "Tom Hank", "star", False, 1972),
        actors(1, "Tom Hank", "good", True, 1973),
        actors(1, "Tom Hank", "star", True, 1974),
        actors(2, "Leonardo Dicaprio", "good", True, 1974)
    ]
    i_schema = StructType([
        StructField("actor_id", IntegerType()),
        StructField("actor", StringType()),
        StructField("quality_class", StringType()),
        StructField("is_active", BooleanType()),
        StructField("current_year", IntegerType()),
    ])
    input_df = spark.createDataFrame(input_data, i_schema)
    actual_df = do_scd_generation(spark, input_df)

    expected_data = [
        actors_scd(1, "Tom Hank", "star", True, 1970, 1971, 1974),
        actors_scd(1, "Tom Hank", "star", False, 1972, 1972, 1974),
        actors_scd(1, "Tom Hank", "good", True, 1973, 1973, 1974),
        actors_scd(1, "Tom Hank", "star", True, 1974, 1974, 1974),
        actors_scd(2, "Leonardo Dicaprio", "good", True, 1974, 1974, 1974)
    ]
    e_schema = StructType([
        StructField("actor_id", IntegerType()),
        StructField("actor", StringType()),
        StructField("quality_class", StringType()),
        StructField("is_active", BooleanType()),
        StructField("start_year", IntegerType()),
        StructField("end_year", IntegerType()),
        StructField("current_year", IntegerType()),
    ])
    expected_df = spark.createDataFrame(expected_data, e_schema)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
