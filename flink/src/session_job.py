from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.window import Session
from pyflink.table.expressions import lit, col

import os


def create_events_source_kafka(t_env: StreamTableEnvironment):
    kafka_url = os.environ.get("KAFKA_URL")
    kafka_topic = os.environ.get("KAFKA_TOPIC")
    kafka_group = os.environ.get("KAFKA_GROUP")
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    time_pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    source_ddl = f"""
        create table {table_name} (
            ip varchar,
            event_time varchar,
            referrer varchar,
            host varchar,
            url varchar,
            geodata varchar,
            window_timestamp as to_timestamp(event_time, '{time_pattern}'),
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
    t_env.execute_sql(source_ddl)
    return table_name


def create_events_sink_postgres(t_env: StreamTableEnvironment):
    url = os.environ.get("POSTGRES_URL")
    username = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    table_name = "processed_user_session_events_agg"
    sink_ddl = f"""
        create table {table_name} (
            event_time timestamp(3),
            ip varchar,
            host varchar,
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


def processing():
    e_env = StreamExecutionEnvironment.get_execution_environment()
    e_env.enable_checkpointing(1000 * 10)
    e_env.set_parallelism(3)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(e_env, settings)

    try:
        source = create_events_source_kafka(t_env)
        sink = create_events_sink_postgres(t_env)

        (
            t_env.from_path(source)
            .window(Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("w"))
            .group_by(
                col("w"),
                col("ip"),
                col("host")
            )
            .select(
                col("w").start.alias("event_time"),
                col("ip"),
                col("host"),
                col("host").count.alias("num_hits")
            )
            .execute_insert(sink)
            .wait()
        )
    except Exception as e:
        print(f"error: {e}")


if __name__ == "__main__":
    # run "make sess_job" in your terminal to submit the job
    processing()
