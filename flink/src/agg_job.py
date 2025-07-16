import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Tumble


def create_agg_events_sink_postgres(t_env: StreamTableEnvironment):
    url = os.environ.get("POSTGRES_URL")
    username = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    table_name = "processed_events_aggregated"
    sink_ddl = f"""
        create table {table_name} (
            event_hour timestamp(3),
            host varchar,
            num_hits bigint
        ) with (
            'connector' = 'jdbc',
            'url' = '{url}',
            'username' = '{username}',
            'password' = '{password}',
            'table-name' = '{table_name}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_agg_events_referrer_sink_postgres(t_env: StreamTableEnvironment):
    url = os.environ.get("POSTGRES_URL")
    username = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    table_name = "processed_events_aggregated_referrer"
    sink_ddl = f"""
        create table {table_name} (
            event_hour timestamp(3),
            host varchar,
            referrer varchar,
            num_hits bigint
        ) with (
            'connector' = 'jdbc',
            'url' = '{url}',
            'username' = '{username}',
            'password' = '{password}',
            'table-name' = '{table_name}',
            'driver' = 'org.postgresql.Driver'
        )
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_source_kafka(t_env: StreamTableEnvironment):
    kafka_url = os.environ.get("KAFKA_URL")
    kafka_topic = os.environ.get("KAFKA_TOPIC")
    kafka_group = os.environ.get("KAFKA_GROUP")
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    date_pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_ddl = f"""
        create table {table_name} (
            ip varchar,
            event_time varchar,
            referrer varchar,
            host varchar,
            url varchar,
            geodata varchar,
            window_timestamp as to_timestamp(event_time, '{date_pattern}'),
            watermark for window_timestamp as window_timestamp - interval '15' second
        ) with (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{kafka_url}',
            'topic' = '{kafka_topic}',
            'properties.group.id' = '{kafka_group}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        )
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def processing():
    # exec env
    e_env = StreamExecutionEnvironment.get_execution_environment()
    e_env.enable_checkpointing(10 * 1000)
    e_env.set_parallelism(3)

    # table env
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(e_env, environment_settings=settings)

    try:
        source_table = create_processed_events_source_kafka(t_env)
        agg_sink_table = create_agg_events_sink_postgres(t_env)
        agg_ref_sink_table = create_agg_events_referrer_sink_postgres(t_env)

        (
            t_env.from_path(source_table)
            .window(
                Tumble.over(lit(5).minutes).on(col("window_timestamp")).alias("w")
            )
            .group_by(
                col("w"),
                col("host")
            )
            .select(
                col("w").start.alias("event_hour"),
                col("host"),
                col("host").count.alias("num_hits")
            )
            .execute_insert(agg_sink_table)
        )

        (
            t_env.from_path(source_table)
            .window(
                Tumble.over(lit(5).minutes).on(col("window_timestamp")).alias("w")
            )
            .group_by(
                col("w"),
                col("host"),
                col("referrer")
            )
            .select(
                col("w").start.alias("event_hour"),
                col("host"),
                col("referrer"),
                col("host").count.alias("num_hits")
            )
            .execute_insert(agg_ref_sink_table)
            .wait()
        )

    except Exception as e:
        print(f"error: {e}")


if __name__ == "__main__":
    processing()
