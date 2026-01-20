"""
Spark script to read from PostgreSQL, process/aggregate data, and write to ClickHouse Cloud

Run with:
docker exec -it spark_master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.1,com.clickhouse:clickhouse-jdbc:0.8.0 \
  /opt/spark/scripts/postgres_to_clickhouse.py

Environment variables (or set in script):
- CLICKHOUSE_HOST: ClickHouse Cloud host (e.g., xyz.clickhouse.cloud)
- CLICKHOUSE_PORT: Port (default: 8443 for HTTPS)
- CLICKHOUSE_DATABASE: Database name (default: default)
- CLICKHOUSE_USER: Username
- CLICKHOUSE_PASSWORD: Password
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, 
    min as spark_min, when, collect_list, first
)

# ClickHouse Cloud configuration
# Set these environment variables or modify the values below
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "your-host.clickhouse.cloud")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8443")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

# Validate configuration
if CLICKHOUSE_HOST == "your-host.clickhouse.cloud" or not CLICKHOUSE_PASSWORD:
    print("=" * 80)
    print("ERROR: ClickHouse Cloud credentials not configured!")
    print("=" * 80)
    print("Please set the following environment variables:")
    print("  - CLICKHOUSE_HOST: Your ClickHouse Cloud host")
    print("  - CLICKHOUSE_USER: Your ClickHouse Cloud username")
    print("  - CLICKHOUSE_PASSWORD: Your ClickHouse Cloud password")
    print("  - CLICKHOUSE_DATABASE: Database name (optional, defaults to 'default')")
    print("  - CLICKHOUSE_PORT: Port (optional, defaults to '8443')")
    print("\nExample:")
    print("  export CLICKHOUSE_HOST=xyz.clickhouse.cloud")
    print("  export CLICKHOUSE_USER=default")
    print("  export CLICKHOUSE_PASSWORD=your_password")
    print("\nOr modify the script directly to set these values.")
    sys.exit(1)

# Create Spark session
spark = SparkSession.builder \
    .appName("PostgreSQLToClickHouse") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1,com.clickhouse:clickhouse-jdbc:0.8.0") \
    .getOrCreate()

print("=" * 80)
print("Reading data from PostgreSQL...")
print("=" * 80)

# Read content table from PostgreSQL
df_content = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
    .option("dbtable", "content") \
    .option("user", "postgres") \
    .option("password", "mysecretpassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

print(f"Content records: {df_content.count()}")

# Read engagement_events table from PostgreSQL
df_engagement = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
    .option("dbtable", "engagement_events") \
    .option("user", "postgres") \
    .option("password", "mysecretpassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

print(f"Engagement events: {df_engagement.count()}")

print("\n" + "=" * 80)
print("Processing and aggregating data...")
print("=" * 80)

# Join content with engagement events
df_joined = df_content.join(
    df_engagement,
    df_content.id == df_engagement.content_id,
    "left"
)

# Aggregate engagement metrics per content
df_aggregated = df_joined.groupBy(
    df_content.id.alias("content_id"),
    df_content.slug.alias("content_slug"),
    df_content.title.alias("content_title"),
    df_content.content_type.alias("content_type"),
    df_content.length_seconds.alias("content_length_seconds"),
    df_content.publish_ts.alias("content_publish_ts")
).agg(
    count(df_engagement.id).alias("total_events"),
    count(when(df_engagement.event_type == "play", 1)).alias("play_count"),
    count(when(df_engagement.event_type == "pause", 1)).alias("pause_count"),
    count(when(df_engagement.event_type == "finish", 1)).alias("finish_count"),
    count(when(df_engagement.event_type == "click", 1)).alias("click_count"),
    spark_sum(df_engagement.duration_ms).alias("total_duration_ms"),
    avg(df_engagement.duration_ms).alias("avg_duration_ms"),
    spark_max(df_engagement.event_ts).alias("last_event_ts"),
    spark_min(df_engagement.event_ts).alias("first_event_ts"),
    count(df_engagement.user_id).alias("unique_users")
)

# Add completion rate (finish_count / play_count)
df_aggregated = df_aggregated.withColumn(
    "completion_rate",
    when(col("play_count") > 0, col("finish_count") / col("play_count")).otherwise(0.0)
)

# Show aggregated data
print("\nAggregated data preview:")
df_aggregated.show(truncate=False)

print("\n" + "=" * 80)
print("Writing aggregated data to ClickHouse Cloud...")
print("=" * 80)

# Construct ClickHouse Cloud JDBC URL with SSL
clickhouse_url = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}?ssl=true"
print(f"Connecting to ClickHouse Cloud: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}")

# Write aggregated data to ClickHouse Cloud
df_aggregated.write \
    .format("jdbc") \
    .option("url", clickhouse_url) \
    .option("dbtable", "content_engagement_metrics") \
    .option("user", CLICKHOUSE_USER) \
    .option("password", CLICKHOUSE_PASSWORD) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("createTableColumnTypes", 
            "content_id String, content_slug String, content_title String, "
            "content_type String, content_length_seconds Int32, "
            "content_publish_ts DateTime64(3), total_events Int64, "
            "play_count Int64, pause_count Int64, finish_count Int64, "
            "click_count Int64, total_duration_ms Int64, avg_duration_ms Float64, "
            "last_event_ts DateTime64(3), first_event_ts DateTime64(3), "
            "unique_users Int64, completion_rate Float64") \
    .mode("overwrite") \
    .save()

print("✓ Successfully wrote aggregated data to ClickHouse table: content_engagement_metrics")

# Also create a daily aggregation for time-series analysis
print("\nCreating daily aggregation...")

df_daily = df_joined.groupBy(
    df_content.id.alias("content_id"),
    df_content.slug.alias("content_slug"),
    df_content.title.alias("content_title"),
    df_content.content_type.alias("content_type"),
    df_engagement.event_ts.cast("date").alias("event_date")
).agg(
    count(df_engagement.id).alias("events_count"),
    count(when(df_engagement.event_type == "play", 1)).alias("play_count"),
    count(when(df_engagement.event_type == "finish", 1)).alias("finish_count"),
    spark_sum(df_engagement.duration_ms).alias("total_duration_ms"),
    count(df_engagement.user_id).alias("unique_users")
)

df_daily = df_daily.filter(col("event_date").isNotNull())

print("\nDaily aggregation preview:")
df_daily.show(truncate=False)

# Write daily aggregation to ClickHouse Cloud
df_daily.write \
    .format("jdbc") \
    .option("url", clickhouse_url) \
    .option("dbtable", "content_engagement_daily") \
    .option("user", CLICKHOUSE_USER) \
    .option("password", CLICKHOUSE_PASSWORD) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("createTableColumnTypes",
            "content_id String, content_slug String, content_title String, "
            "content_type String, event_date Date, events_count Int64, "
            "play_count Int64, finish_count Int64, total_duration_ms Int64, "
            "unique_users Int64") \
    .mode("overwrite") \
    .save()

print("✓ Successfully wrote daily aggregation to ClickHouse table: content_engagement_daily")

print("\n" + "=" * 80)
print("Processing complete!")
print("=" * 80)
print(f"\nData successfully written to ClickHouse Cloud: {CLICKHOUSE_HOST}")
print("Tables created:")
print("  - content_engagement_metrics (overall metrics per content)")
print("  - content_engagement_daily (daily metrics per content)")

spark.stop()
