"""
Pipeline batch-like (Spark Structured Streaming with triggerOnce)
Leitura: Kafka topics -> raw (parquet) -> validação/enriquecimento -> processed (parquet) -> curated/latest (parquet)
DLQ: registros inválidos (parquet)

Requirements / Assumptions:
- Spark (PySpark) disponível no container
- Acesso ao Kafka em kafka:9092 (nome do serviço no docker-compose)
- Credenciais AWS disponíveis no ambiente (IAM role, AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, or Hadoop/AWS profile)
- S3 URL via s3a://data-mesh-clients
- Tamanho do batch pequeno (protótipo). For production consider Delta Lake for merges.
"""

import sys
import json
import os
import re
import unicodedata
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, IntegerType

# ---------- Configs ----------
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC_PATTERN = "clients\\.(create|update|delete)\\.v1"  # subscribePattern
S3_BUCKET = "s3a://data-mesh-clients"
RAW_PATH = f"{S3_BUCKET}/raw/clients"
PROCESSED_PATH = f"{S3_BUCKET}/processed/clients"
CURATED_LATEST_PATH = f"{S3_BUCKET}/curated/clients/latest"
DLQ_PATH = f"{S3_BUCKET}/dlq/clients"
CHECKPOINT_BASE = "/tmp/spark_checkpoints/client_pipeline"  # ephemeral for prototype

# Simple email regex (prototype)
EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

# ---------- Helpers ----------
def create_spark(app_name="clientes_batch_pipeline"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.profile.ProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.aws.profile", "data-mesh")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    return spark

def path_exists(spark: SparkSession, path: str) -> bool:
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))
    except Exception:
        return False

# Normalization helpers (use udf to remove accents)
def remove_accents_py(s: str) -> str:
    if s is None:
        return None
    normalized = unicodedata.normalize("NFKD", s)
    return "".join([c for c in normalized if not unicodedata.combining(c)])

# Register udf in spark later

# ---------- Main pipeline ----------
def main():
    spark = create_spark()

    # register python udf
    remove_accents = F.udf(lambda x: remove_accents_py(x) if x is not None else None, StringType())

    # read from kafka as streaming DataFrame
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribePattern", TOPIC_PATTERN) \
        .option("startingOffsets", "earliest") \
        .load()

    # kafka key/value are binary -> cast to string
    df = kafka_df.selectExpr(
        "CAST(topic AS STRING) as source_topic",
        "CAST(key AS STRING) as key",
        "CAST(value AS STRING) as value",
        "timestamp as kafka_timestamp"
    )

    # schema for JSON payload
    payload_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("client_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("timestamp", DoubleType(), True)  # epoch seconds
    ])

    parsed = df.withColumn("json", F.from_json(F.col("value"), payload_schema)) \
        .select(
            "source_topic",
            "key",
            "value",
            "kafka_timestamp",
            F.col("json.event_id").alias("event_id"),
            F.col("json.client_id").alias("client_id"),
            F.col("json.name").alias("name"),
            F.col("json.email").alias("email"),
            F.col("json.timestamp").alias("event_timestamp")
        )

    # Save raw messages (append) partitioned by date (date of processing = current_date)
    enriched_for_raw = parsed.withColumn("ingest_ts", F.current_timestamp()) \
        .withColumn("year", F.date_format(F.col("ingest_ts"), "yyyy")) \
        .withColumn("month", F.date_format(F.col("ingest_ts"), "MM")) \
        .withColumn("day", F.date_format(F.col("ingest_ts"), "dd"))

    raw_query = enriched_for_raw.writeStream \
        .format("parquet") \
        .option("path", RAW_PATH) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw") \
        .partitionBy("year", "month", "day") \
        .outputMode("append") \
        .trigger(once=True) \
        .start()

    # Wait for raw write (triggerOnce will process available data then stop)
    raw_query.awaitTermination()

    # After raw saved, continue processing on the same micro-batch by turning parsed into a static DataFrame
    # We'll read the files we just wrote for deterministic batch processing OR use the parsed DataFrame collected from stream.
    # Simpler: use parsed as a micro-batch snapshot by reading the same data as a static DF via read from Kafka (one-time).
    # For robustness, read with static option:
    static_kafka = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribePattern", TOPIC_PATTERN) \
        .option("startingOffsets", "earliest") \
        .load()

    static_df = static_kafka.selectExpr(
        "CAST(topic AS STRING) as source_topic",
        "CAST(key AS STRING) as key",
        "CAST(value AS STRING) as value",
        "timestamp as kafka_timestamp"
    )

    static_parsed = static_df.withColumn("json", F.from_json(F.col("value"), payload_schema)) \
        .select(
            "source_topic",
            "key",
            "value",
            "kafka_timestamp",
            F.col("json.event_id").alias("event_id"),
            F.col("json.client_id").alias("client_id"),
            F.col("json.name").alias("name"),
            F.col("json.email").alias("email"),
            F.col("json.timestamp").alias("event_timestamp")
        )

    # Normalization
    normalized = static_parsed \
        .withColumn("name_trim", F.trim(F.col("name"))) \
        .withColumn("name_no_acc", remove_accents(F.col("name_trim"))) \
        .withColumn("name_norm", F.initcap(F.lower(F.col("name_no_acc")))) \
        .withColumn("email_trim", F.trim(F.col("email"))) \
        .withColumn("email_no_acc", remove_accents(F.col("email_trim"))) \
        .withColumn("email_norm", F.lower(F.col("email_no_acc"))) \
        .withColumn("processed_at", F.current_timestamp()) \
        .withColumn("ingestion_date", F.to_date(F.col("processed_at")))

    # Validation -> produce a flag column invalid_reason (nullable)
    cond_email_valid = F.regexp_extract(F.col("email_norm"), EMAIL_REGEX, 0) != ""
    cond_client_id_valid = F.col("client_id").isNotNull() & (F.col("client_id").cast("int").isNotNull())
    cond_name_valid = (F.col("name_norm").isNotNull()) & (F.length(F.col("name_norm")) > 0)

    validated = normalized.withColumn(
        "invalid_reason",
        F.when(~cond_email_valid, F.lit("invalid_email"))
         .when(~cond_client_id_valid, F.lit("invalid_client_id"))
         .when(~cond_name_valid, F.lit("invalid_name"))
         .otherwise(F.lit(None))
    )

    # Separate valid / invalid
    invalid_df = validated.filter(F.col("invalid_reason").isNotNull()) \
        .withColumn("year", F.date_format(F.col("processed_at"), "yyyy")) \
        .withColumn("month", F.date_format(F.col("processed_at"), "MM")) \
        .withColumn("day", F.date_format(F.col("processed_at"), "dd")) \
        .select("event_id", "client_id", "name", "email", "value", "invalid_reason", "processed_at", "source_topic", "year", "month", "day")

    valid_df = validated.filter(F.col("invalid_reason").isNull()) \
        .withColumn("year", F.date_format(F.col("processed_at"), "yyyy")) \
        .withColumn("month", F.date_format(F.col("processed_at"), "MM")) \
        .withColumn("day", F.date_format(F.col("processed_at"), "dd")) \
        .select(
            "event_id",
            "client_id",
            F.col("name_norm").alias("name"),
            F.col("email_norm").alias("email"),
            "event_timestamp",
            "processed_at",
            "source_topic",
            "year", "month", "day"
        )

    # Write invalids to DLQ (append)
    if invalid_df.rdd.isEmpty():
        # no invalids -> skip write to avoid creating empty files (optional)
        pass
    else:
        invalid_df.write.mode("append").partitionBy("year", "month", "day").parquet(DLQ_PATH)

    # Write processed valid records to processed zone (append by ingestion date)
    if valid_df.rdd.isEmpty():
        print("No valid records found in this batch.")
    else:
        valid_df.write.mode("append").partitionBy("year", "month", "day").parquet(PROCESSED_PATH)

    # Update curated/latest (stateful: keep most recent record per client_id)
    # Strategy (prototype):
    # - Read existing curated/latest if exists
    # - Union with new valid_df
    # - Deduplicate keeping the record with latest processed_at
    try:
        if path_exists(spark, CURATED_LATEST_PATH):
            existing_latest = spark.read.parquet(CURATED_LATEST_PATH)
            combined = existing_latest.unionByName(valid_df.select(existing_latest.columns) if set(existing_latest.columns).issubset(set(valid_df.columns)) else valid_df, allowMissingColumns=True)
        else:
            combined = valid_df
    except Exception as e:
        # if read fails, fallback to only new batch
        combined = valid_df

    if combined.rdd.isEmpty():
        print("No records to upsert into curated latest.")
    else:
        w = Window.partitionBy("client_id").orderBy(F.col("processed_at").desc())
        deduped = combined.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
        # write overwrite the curated latest path
        deduped.write.mode("overwrite").parquet(CURATED_LATEST_PATH)

    spark.stop()
    print("Batch run finished.")

if __name__ == "__main__":
    main()
