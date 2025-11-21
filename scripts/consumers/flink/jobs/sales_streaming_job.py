"""
flink_sales_table_job.py

PyFlink Table API streaming job:
 - Kafka JSON (3 topics) -> normalize -> write RAW + TRUSTED into S3 (Parquet, SNAPPY)
 - Partitioning: year/month/day (computed from timestamp)
 - Assumes AWS credentials available to Flink (e.g. /root/.aws/credentials or environment vars)
"""

from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment,
)
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration


def main():
    # ---------- optional: use a StreamExecutionEnvironment to enable checkpointing ----------
    stream_env = StreamExecutionEnvironment.get_execution_environment()
    stream_env.set_parallelism(1)
    # If you want checkpointing (exactly-once semantics), uncomment and tune:
    # stream_env.enable_checkpointing(30000)  # checkpoint every 30s
    # stream_env.get_checkpoint_config().set_min_pause_between_checkpoints(10000)

    # ---------- Table environment in streaming mode ----------
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(env_settings)

    # Ensure the table env uses the stream env's config for checkpointing if you enabled it
    # (PyFlink bridges some configs automatically when submitting via flink run -py)

    # ---------- (Optional) S3 settings (if you prefer to set via config instead of ~/.aws) ----------
    # NOTE: recommended to keep real AWS creds in /root/.aws/credentials or env vars.
    # Example (uncomment if you prefer):
    # cfg = t_env.get_config().get_configuration()
    # cfg.set_string("fs.s3a.access.key", "YOUR_ACCESS_KEY")
    # cfg.set_string("fs.s3a.secret.key", "YOUR_SECRET_KEY")
    # cfg.set_string("fs.s3a.endpoint", "s3.amazonaws.com")
    # cfg.set_string("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # ---------- Kafka bootstrap and S3 base path (adjust as needed) ----------
    BOOTSTRAP_SERVERS = "kafka:9092"
    S3_BASE = "s3a://data-mesh-sells"

    # ---------- CREATE KAFKA SOURCES (JSON) ----------
    # sales.create.v1
    t_env.execute_sql(f"""
        CREATE TABLE sales_create (
            event_id STRING,
            sale_id INT,
            client_id INT,
            price DOUBLE,
            items INT,
            `timestamp` TIMESTAMP(3),
            `year` AS YEAR(`timestamp`),
            `month` AS MONTH(`timestamp`),
            `day` AS DAYOFMONTH(`timestamp`)
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales.create.v1',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-sales-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # sales.update.v1
    t_env.execute_sql(f"""
        CREATE TABLE sales_update (
            event_id STRING,
            sale_id INT,
            changed_field STRING,
            new_value STRING,
            `timestamp` TIMESTAMP(3),
            `year` AS YEAR(`timestamp`),
            `month` AS MONTH(`timestamp`),
            `day` AS DAYOFMONTH(`timestamp`)
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales.update.v1',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-sales-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # sales.cancel.v1
    t_env.execute_sql(f"""
        CREATE TABLE sales_cancel (
            event_id STRING,
            sale_id INT,
            reason STRING,
            `timestamp` TIMESTAMP(3),
            `year` AS YEAR(`timestamp`),
            `month` AS MONTH(`timestamp`),
            `day` AS DAYOFMONTH(`timestamp`)
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales.cancel.v1',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-sales-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # ---------- SINKS: filesystem (Parquet) for RAW and TRUSTED ----------
    # RAW layer: store the original payload as a JSON string (one column payload)
    t_env.execute_sql(f"""
        CREATE TABLE sales_raw (
            event_id STRING,
            sale_id INT,
            event_type STRING,
            payload STRING,
            `timestamp` TIMESTAMP(3),
            `year` STRING,
            `month` STRING,
            `day` STRING
        ) PARTITIONED BY (`year`,`month`,`day`)
        WITH (
            'connector' = 'filesystem',
            'path' = '{S3_BASE}/raw/sales/',
            'format' = 'parquet',
            'parquet.compression' = 'SNAPPY'
        )
    """)

    # TRUSTED layer: parsed columns for analytics
    t_env.execute_sql(f"""
        CREATE TABLE sales_trusted (
            event_id STRING,
            sale_id INT,
            client_id INT,
            price DOUBLE,
            items INT,
            changed_field STRING,
            new_value STRING,
            reason STRING,
            event_type STRING,
            `timestamp` TIMESTAMP(3),
            `year` STRING,
            `month` STRING,
            `day` STRING
        ) PARTITIONED BY (`year`,`month`,`day`)
        WITH (
            'connector' = 'filesystem',
            'path' = '{S3_BASE}/trusted/sales/',
            'format' = 'parquet',
            'parquet.compression' = 'SNAPPY'
        )
    """)

    # ---------- Unified VIEW to normalize different event types ----------
    t_env.execute_sql("""
        CREATE VIEW all_sales AS
        SELECT
            CAST(event_id AS STRING) AS event_id,
            CAST(sale_id AS STRING) AS sale_id,
            CAST(client_id AS STRING) AS client_id,
            CAST(price AS STRING) AS price,
            CAST(items AS STRING) AS items,
            CAST(NULL AS STRING) AS changed_field,
            CAST(NULL AS STRING) AS new_value,
            CAST(NULL AS STRING) AS reason,
            'CREATE' AS event_type,
            CAST(`timestamp` AS STRING) AS `timestamp`,
            CAST(`year` AS STRING) AS `year`,
            CAST(`month` AS STRING) AS `month`,
            CAST(`day` AS STRING) AS `day`,
            JSON_STRING(
                JSON_OBJECT(
                    'event_id' VALUE event_id,
                    'sale_id' VALUE sale_id,
                    'client_id' VALUE client_id,
                    'price' VALUE price,
                    'items' VALUE items,
                    'timestamp' VALUE `timestamp`
                )
            ) AS raw_payload
        FROM sales_create

        UNION ALL

        SELECT
            CAST(event_id AS STRING) AS event_id,
            CAST(sale_id AS STRING) AS sale_id,
            CAST(NULL AS STRING) AS client_id,
            CAST(NULL AS STRING) AS price,
            CAST(NULL AS STRING) AS items,
            CAST(changed_field AS STRING) AS changed_field,
            CAST(new_value AS STRING) AS new_value,
            CAST(NULL AS STRING) AS reason,
            'UPDATE' AS event_type,
            CAST(`timestamp` AS STRING) AS `timestamp`,
            CAST(`year` AS STRING) AS `year`,
            CAST(`month` AS STRING) AS `month`,
            CAST(`day` AS STRING) AS `day`,
            JSON_STRING(
                JSON_OBJECT(
                    'event_id' VALUE event_id,
                    'sale_id' VALUE sale_id,
                    'changed_field' VALUE changed_field,
                    'new_value' VALUE new_value,
                    'timestamp' VALUE `timestamp`
                )
            ) AS raw_payload
        FROM sales_update

        UNION ALL

        SELECT
            CAST(event_id AS STRING) AS event_id,
            CAST(sale_id AS STRING) AS sale_id,
            CAST(NULL AS STRING) AS client_id,
            CAST(NULL AS STRING) AS price,
            CAST(NULL AS STRING) AS items,
            CAST(NULL AS STRING) AS changed_field,
            CAST(NULL AS STRING) AS new_value,
            CAST(reason AS STRING) AS reason,
            'CANCEL' AS event_type,
            CAST(`timestamp` AS STRING) AS `timestamp`,
            CAST(`year` AS STRING) AS `year`,
            CAST(`month` AS STRING) AS `month`,
            CAST(`day` AS STRING) AS `day`,
            JSON_STRING(
                JSON_OBJECT(
                    'event_id' VALUE event_id,
                    'sale_id' VALUE sale_id,
                    'reason' VALUE reason,
                    'timestamp' VALUE `timestamp`
                )
            ) AS raw_payload
        FROM sales_cancel
    """)

    # ---------- INSERT into RAW (raw_payload as string) ----------
    t_env.execute_sql("""
        INSERT INTO sales_raw
        SELECT
            event_id,
            CAST(sale_id AS INT) AS sale_id,
            event_type,
            raw_payload AS payload,
            CAST(`timestamp` AS TIMESTAMP(3)) AS `timestamp`,
            `year`,
            `month`,
            `day`
        FROM all_sales
    """)

    # ---------- INSERT into TRUSTED (parsed columns) ----------
    t_env.execute_sql("""
        INSERT INTO sales_trusted
        SELECT
            event_id,
            CAST(sale_id AS INT) AS sale_id,
            CAST(client_id AS INT) AS client_id,
            CAST(price AS DOUBLE) AS price,
            CAST(items AS INT) AS items,
            changed_field,
            new_value,
            reason,
            event_type,
            CAST(`timestamp` AS TIMESTAMP(3)) AS `timestamp`,
            `year`,
            `month`,
            `day`
        FROM all_sales
    """)

    # Note: The INSERT statements submit the streaming jobs.
    # When invoked via `flink run -py`, the process will stay running as the streaming job runs.

if __name__ == "__main__":
    main()
