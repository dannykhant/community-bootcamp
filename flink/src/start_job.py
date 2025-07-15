import os
import json
import requests

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.udf import ScalarFunction, udf
from pyflink.table import (
    EnvironmentSettings,
    DataTypes,
    StreamTableEnvironment
)


class GetLocation(ScalarFunction):
    def eval(self, ip_addr):
        url = "https://api.ip2location.io"
        response = requests.get(url, params={
            "ip": ip_addr,
            "key": os.environ.get("IP_CODING_KEY", "")
        })

        if response.status_code != 200:
            return json.dumps({})

        data = json.loads(response.text)
        country = data.get("country_code", "")
        state = data.get("region_name", "")
        city = data.get("city_name", "")

        return json.dumps({
            "country": country,
            "state": state,
            "city": city
        })


def create_events_source_kafka(t_env: StreamTableEnvironment):
    kafka_url = os.environ.get("KAFKA_URL")
    kafka_topic = os.environ.get("KAFKA_TOPIC")
    kafka_group = os.environ.get("KAFKA_GROUP")
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events"
    date_pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    source_ddl = f"""
        create table {table_name} (
            url varchar,
            referrer varchar,
            user_agent varchar,
            host varchar,
            ip varchar,
            headers varchar,
            event_time varchar,
            event_timestamp as to_timestamp(event_time, '{date_pattern}')
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
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_processed_events_sink_postgres(t_env: StreamTableEnvironment):
    postgres_url = os.environ.get("POSTGRES_URL")
    postgres_username = os.environ.get("POSTGRES_USER", "postgres")
    postgres_password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    table_name = "processed_events"
    sink_ddl = f"""
        create table {table_name} (
            ip varchar,
            event_timestamp timestamp(3),
            referrer varchar,
            host varchar,
            url varchar,
            geodata varchar
        ) with (
            'connector' = 'jdbc',
            'url' = '{postgres_url}',
            'table-name' = '{table_name}',
            'username' = '{postgres_username}',
            'password' = '{postgres_password}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def processing():
    # set up the execution env
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    # set up the table env
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    get_location_func = udf(GetLocation(), result_type=DataTypes.STRING())
    t_env.create_temporary_function("get_location", get_location_func)

    try:
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_processed_events_sink_postgres(t_env)
        t_env.execute_sql(f"""
            insert into {postgres_sink}
            select
                ip,
                event_timestamp,
                referrer,
                host,
                url,
                get_location(ip) as geodata
            from {source_table}
        """).wait()
    except Exception as e:
        print("error: ", e)


if __name__ == "__main__":
    processing()
