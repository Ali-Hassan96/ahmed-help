"""
Spark Structured Streaming from Kafka to ClickHouse Cloud

This is a more advanced streaming setup using Kafka as the event source.
Kafka provides true event streaming capabilities.

Prerequisites:
1. Kafka cluster (can be added to compose.yaml)
2. Events published to Kafka topics
3. ClickHouse Cloud credentials configured

Run with:
docker exec -it spark_master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.clickhouse:clickhouse-jdbc:0.8.0 \
  /opt/spark/scripts/kafka_to_clickhouse_streaming.py

Environment variables:
- KAFKA_BOOTSTRAP_SERVERS: Kafka brokers (e.g., kafka:9092)
- KAFKA_TOPIC: Topic name (e.g., engagement_events)
- CLICKHOUSE_HOST: ClickHouse Cloud host
- CLICKHOUSE_USER: ClickHouse username
- CLICKHOUSE_PASSWORD: ClickHouse password
- STREAMING_INTERVAL: Processing interval (default: 10 seconds)
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, window, current_timestamp, 
    count, sum as spark_sum, avg
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, LongType
)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "engagement_events")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "your-host.clickhouse.cloud")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8443")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
STREAMING_INTERVAL = os.getenv("STREAMING_INTERVAL", "10 seconds")

# Validate
if CLICKHOUSE_HOST == "your-host.clickhouse.cloud" or not CLICKHOUSE_PASSWORD:
    print("ERROR: ClickHouse Cloud credentials not configured!")
    sys.exit(1)

# Create Spark session with Kafka support
spark = SparkSession.builder \
    .appName("KafkaToClickHouseStreaming") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "com.clickhouse:clickhouse-jdbc:0.8.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint-kafka-clickhouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("Spark Structured Streaming: Kafka → ClickHouse Cloud")
print("=" * 80)
print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Topic: {KAFKA_TOPIC}")
print(f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}")
print(f"Interval: {STREAMING_INTERVAL}")

# Define schema for engagement events
event_schema = StructType([
    StructField("id", LongType(), True),
    StructField("content_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("device", StringType(), True),
    StructField("raw_payload", StringType(), True)
])

# Read from Kafka
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON from Kafka value
df_events = df_kafka.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("json_value"),
    col("timestamp").alias("kafka_timestamp"),
    col("partition"),
    col("offset")
).withColumn(
    "event",
    from_json(col("json_value"), event_schema)
).select(
    "kafka_key",
    "kafka_timestamp",
    "partition",
    "offset",
    "event.*"
).withColumn(
    "processed_at",
    current_timestamp()
)

# ClickHouse URL
clickhouse_url = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}?ssl=true"

# Write to ClickHouse using foreachBatch
def write_to_clickhouse_batch(batch_df, epoch_id):
    """Write each micro-batch to ClickHouse"""
    try:
        count = batch_df.count()
        if count > 0:
            print(f"\n📊 Processing batch {epoch_id}: {count} events")
            batch_df.show(5, truncate=False)
            
            batch_df.write \
                .format("jdbc") \
                .option("url", clickhouse_url) \
                .option("dbtable", "engagement_events_stream") \
                .option("user", CLICKHOUSE_USER) \
                .option("password", CLICKHOUSE_PASSWORD) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .option("createTableColumnTypes",
                        "id Int64, content_id String, user_id String, event_type String, "
                        "event_ts DateTime64(3), duration_ms Int32, device String, "
                        "raw_payload String, processed_at DateTime64(3)") \
                .mode("append") \
                .save()
            
            print(f"✅ Batch {epoch_id} written to ClickHouse")
    except Exception as e:
        print(f"❌ Error in batch {epoch_id}: {e}")
        import traceback
        traceback.print_exc()

# Start streaming query
print("\n🚀 Starting streaming query...")
print("Press Ctrl+C to stop\n")

query = df_events.writeStream \
    .foreachBatch(write_to_clickhouse_batch) \
    .outputMode("append") \
    .trigger(processingTime=STREAMING_INTERVAL) \
    .start()

# Wait for termination
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n🛑 Stopping streaming query...")
    query.stop()
    spark.stop()
    print("✅ Streaming stopped")
