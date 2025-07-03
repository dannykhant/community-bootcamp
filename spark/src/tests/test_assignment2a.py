from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, ArrayType, DateType
from pyspark.sql.functions import col

from ..jobs.assignment2a import do_datelist_generation
from collections import namedtuple
from datetime import datetime

user_devices = namedtuple(
    "UserDevices",
    "user_id device_id date device_activity_datelist"
)

user_devices_datelist = namedtuple(
    "UserDevicesDT",
    "user_id device_id datalist_int"
)


def to_date(dstr):
    return datetime.strptime(dstr, "%Y-%m-%d").date()


def test_datelist(spark):
    input_data = [
        user_devices(1, 1, to_date("2023-01-31"), {
            "browser_type": "firefox",
            "datelist": [to_date("2023-01-01"), to_date("2023-01-05"), to_date("2023-01-11"), to_date("2023-01-30")]}),
        user_devices(2, 1, to_date("2023-01-31"), {
            "browser_type": "firefox",
            "datelist": [to_date("2023-01-31")]})
    ]
    i_schema = StructType([
        StructField("user_id", IntegerType()),
        StructField("device_id", IntegerType()),
        StructField("date", DateType()),
        StructField("device_activity_datelist", StructType([
            StructField("browser_type", StringType()),
            StructField("datelist", ArrayType(DateType()))
        ]))
    ])
    input_df = spark.createDataFrame(input_data, i_schema)
    actual_df = do_datelist_generation(spark, input_df).sortWithinPartitions(col("user_id"))

    expected_data = [
        user_devices_datelist(1, 1, {"browser_type": "firefox", "intlist": "01000000000000000000100000100010"}),
        user_devices_datelist(2, 1, {"browser_type": "firefox", "intlist": "10000000000000000000000000000000"})
    ]
    e_schema = StructType([
        StructField("user_id", IntegerType()),
        StructField("device_id", IntegerType()),
        StructField("datelist_int", StructType([
            StructField("browser_type", StringType()),
            StructField("intlist", StringType())
        ]))
    ])
    expected_df = spark.createDataFrame(expected_data, e_schema)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
