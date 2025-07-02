from pyspark.sql import SparkSession


def do_monthly_user_site_hits_transformation(spark, df, ds):
    query = f"""
        select
            month_start,
            sum(coalesce(get(hit_array, 0), 0)) as num_hits_first_day,
            sum(coalesce(get(hit_array, 1), 0)) as num_hits_second_day,
            sum(coalesce(get(hit_array, 2), 0)) as num_hits_third_day
        from monthly_user_site_hits
        where date_partition = '{ds}'
        group by month_start
    """
    df.createOrReplaceTempView("monthly_user_site_hits")
    return spark.sql(query)


def main():
    ds = "2023-01-01"
    spark = SparkSession.builder \
        .master("local").appName("monthly_user_hits").getOrCreate()

    output_df = do_monthly_user_site_hits_transformation(
        spark,
        spark.table("monthly_user_site_hits"),
        ds
    )

    (output_df.write.mode("overwrite")
     .insertInto("monthly_user_site_hits_agg"))
