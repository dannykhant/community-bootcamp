from pyspark.sql import SparkSession

query = f"""
    with previous as (
        select
            actor_id,
            actor,
            quality_class,
            is_active,
            lag(quality_class) over 
                (partition by actor_id order by current_year) as previous_quality_class,
            lag(is_active) over
                (partition by actor_id order by current_year) as previous_is_active,
            current_year
        from actors
    )
    , indicator as (
        select
            *,
            case when quality_class <> previous_quality_class then 1
             when is_active <> previous_is_active then 1
             else 0
            end as indicator
        from previous
    )
    , streak_indicator as (
        select
            *,
            sum(indicator) over (partition by actor_id order by current_year) as streak_indicator
        from indicator
    )
    select
        actor_id,
        actor,
        quality_class,
        is_active,
        min(current_year) as start_year,
        max(current_year) as end_year,
        1974 as current_year
    from streak_indicator
    group by 
        actor_id,
        actor,
        streak_indicator,
        quality_class,
        is_active
    order by 1, streak_indicator
"""


def do_scd_generation(spark: SparkSession, df):
    df.createOrReplaceTempView("actors")
    return spark.sql(query)


def main():
    spark = SparkSession.builder.master("local") \
        .appName("assignment2b").getOrCreate()
    output_df = do_scd_generation(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_scd")


if __name__ == "__main__":
    main()
