from pyspark.sql import SparkSession

query = f"""
    with user_devices as (
        select *
        from user_devices_cumulated
        where date = date('2023-01-31')
    )
    , series as (
        select explode(sequence(to_date('2023-01-01'), to_date('2023-01-31'), interval 1 day)) AS serie_date
    )
    , ints as (
        select *,
            case when array_contains(device_activity_datelist.datelist, serie_date)
                    then pow(2, 31 - datediff(date, serie_date))
                else 0 
            end as int_value
        from user_devices ud
        cross join series s
    )
    select
        user_id,
        device_id,
        struct(device_activity_datelist.browser_type,
            lpad(bin(sum(int_value)), 32, '0') as intlist) as datelist_int
    from ints
    group by user_id, device_id, 
                device_activity_datelist.browser_type
"""


def do_datelist_generation(spark: SparkSession, df):
    df.createOrReplaceTempView("user_devices_cumulated")
    return spark.sql(query)


def main():
    spark = SparkSession.builder.master("local") \
        .appName("assignment2a").getOrCreate()
    output_df = do_datelist_generation(spark, spark.table("user_devices_cumulated"))
    output_df.write.mode("overwrite").insertInto("user_devices_datelist")


if __name__ == "__main__":
    main()
