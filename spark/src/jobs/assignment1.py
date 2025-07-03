from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, avg, count, sum


def do_joins(spark: SparkSession):
    medals = spark.read.option("header", "true") \
        .csv("/home/iceberg/data/medals.csv")
    maps = spark.read.option("header", "true") \
        .csv("/home/iceberg/data/maps.csv")
    matches = spark.read.option("header", "true") \
        .csv("/home/iceberg/data/matches.csv")
    match_details = spark.read.option("header", "true") \
        .csv("/home/iceberg/data/match_details.csv")
    medals_matches_players = spark.read.option("header", "true") \
        .csv("/home/iceberg/data/medals_matches_players.csv")

    spark.sql("""
        create table if not exists bootcamp.matches_bucketed (
            match_id string,
            mapid string,
            playlist_id string,
            completion_date timestamp,
            is_team_game boolean
        )
        using iceberg
        partitioned by (completion_date, bucket(16, match_id))
    """)
    matches.select(
        col("match_id"),
        col("mapid"),
        col("playlist_id"),
        col("completion_date").cast("timestamp"),
        col("is_team_game").cast("boolean")
    ).write.mode("overwrite") \
        .partitionBy("completion_date") \
        .bucketBy(16, "match_id") \
        .saveAsTable("bootcamp.matches_bucketed")

    spark.sql("""
        create table if not exists bootcamp.match_details_bucketed (
            match_id string,
            player_gamertag string,
            spartan_rank integer,
            player_total_kills integer
        )
        using iceberg
        partitioned by (bucket(16, match_id))
    """)
    match_details.select(
        col("match_id"),
        col("player_gamertag"),
        col("spartan_rank"),
        col("player_total_kills")
    ).write.mode("overwrite") \
        .bucketBy(16, "match_id") \
        .saveAsTable("bootcamp.match_details_bucketed")

    spark.sql("""
        create table if not exists bootcamp.medal_matches_players_bucketed (
            match_id string,
            medal_id string,
            count integer
        )
        using iceberg
        partitioned by (bucket(16, match_id))
    """)
    medals_matches_players.select(
        col("match_id"),
        col("medal_id"),
        col("count")
    ).write.mode("overwrite") \
        .bucketBy(16, "match_id") \
        .saveAsTable("bootcamp.medals_matches_players_bucketed")

    # Bucket join
    match_bucket_join = spark.sql("""
        select 
            mb.match_id,
            mb.playlist_id,
            mb.mapid,
            mb.completion_date,
            mdb.player_gamertag,
            mdb.player_total_kills,
            mmp.medal_id,
            mmp.count as medal_count
        from bootcamp.matches_bucketed mb
        join bootcamp.match_details_bucketed mdb
        on mb.match_id = mdb.match_id
        join bootcamp.medals_matches_players_bucketed mmp
        on mb.match_id = mmp.match_id
    """)

    output_df = match_bucket_join.repartition(10).join(
        broadcast(medals).alias("medal"),
        on="medal_id",
        how="inner"
    ).join(
        broadcast(maps).alias("map"),
        on="mapid",
        how="inner"
    )
    return output_df


def analysis(spark: SparkSession, df):
    # Which player averages the most kills per game?
    avg_kills = (df.groupBy("player_gamertag")
                 .agg(avg("player_total_kills").alias("avg_kills"))
                 .orderBy("avg_kills", ascending=False)
                 .limit(1))
    avg_kills.show()

    # Which playlist gets played the most?
    playlist_most_played = (df.groupBy("playlist_id")
                            .agg(count("match_id").alias("num_played"))
                            .orderBy("num_played", ascending=False)
                            .limit(1))
    playlist_most_played.show()

    # Which map gets played the most?
    map_most_played = (df.groupBy("mapid", "map.name")
                       .agg(count("match_id").alias("num_played"))
                       .orderBy("num_played", ascending=False)
                       .limit(1))
    map_most_played.show()

    # Which map do players get the most Killing Spree medals on?
    map_most_played = (df.filter("medal.name == 'Killing Spree'")
                       .groupBy("mapid", "map.name")
                       .agg(sum("medal_count").alias("total_medals"))
                       .orderBy("total_medals", ascending=False)
                       .limit(1))
    map_most_played.show()

    # Check smallest data size
    df_selected = df.select(
        col("match_id"),
        col("playlist_id"),
        col("medal_id"),
        col("mapid")
    )
    df_selected.write.mode("overwrite").saveAsTable("bootcamp.unsorted")

    sorted_one = df_selected.sortWithinPartitions(col("playlist_id"), col("mapid"))
    sorted_one.write.mode("overwrite").saveAsTable("bootcamp.sorted_one")

    sorted_two = df_selected.sortWithinPartitions(col("medal_id"), col("mapid"))
    sorted_two.write.mode("overwrite").saveAsTable("bootcamp.sorted_two")

    size_df = spark.sql("""
        select sum(file_size_in_bytes) as size, 'unsorted' as name
        from bootcamp.unsorted.files
        union all
        select sum(file_size_in_bytes), 'one' as name
        from bootcamp.sorted_one.files
        union all
        select sum(file_size_in_bytes), 'two' as name
        from bootcamp.sorted_two.files
    """)
    size_df.show()


def main():
    spark = (SparkSession.builder
             .master("local")
             .appName("assignment1")
             .getOrCreate())
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.sparkContext.setLogLevel("WARN")

    df = do_joins(spark)
    analysis(spark, df)


if __name__ == "__main__":
    main()
