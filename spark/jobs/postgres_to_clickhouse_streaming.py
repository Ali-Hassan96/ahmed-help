"""
Spark Structured Streaming script to read from PostgreSQL and stream to ClickHouse Cloud

This script uses Spark Structured Streaming to process events in real-time (micro-batches)
and write them to ClickHouse Cloud for analytics.

Run with:
docker exec -it spark_master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.1,com.clickhouse:clickhouse-jdbc:0.8.0 \
  /opt/spark/scripts/postgres_to_clickhouse_streaming.py

Environment variables (or set in script):
- CLICKHOUSE_HOST: ClickHouse Cloud host (e.g., xyz.clickhouse.cloud)
- CLICKHOUSE_PORT: Port (default: 8443 for HTTPS)
- CLICKHOUSE_DATABASE: Database name (default: default)
- CLICKHOUSE_USER: Username
- CLICKHOUSE_PASSWORD: Password
- STREAMING_INTERVAL: Processing interval in seconds (default: 10)
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, 
    min as spark_min, when, window, current_timestamp, from_json, to_json
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# ClickHouse Cloud configuration
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "your-host.clickhouse.cloud")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8443")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
STREAMING_INTERVAL = int(os.getenv("STREAMING_INTERVAL", "10"))  # seconds

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
    print("  - STREAMING_INTERVAL: Processing interval in seconds (optional, defaults to '10')")
    sys.exit(1)

# Create Spark session with streaming support
spark = SparkSession.builder \
    .appName("PostgreSQLToClickHouseStreaming") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1,com.clickhouse:clickhouse-jdbc:0.8.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint-clickhouse") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("Spark Structured Streaming: PostgreSQL → ClickHouse Cloud")
print("=" * 80)
print(f"Streaming interval: {STREAMING_INTERVAL} seconds")
print(f"ClickHouse target: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}")

# ClickHouse JDBC URL
clickhouse_url = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}?ssl=true"

def write_to_clickhouse(batch_df, epoch_id):
    """
    Foreach batch function to write micro-batch data to ClickHouse
    This function is called for each micro-batch in the stream
    """
    try:
        print(f"\n📊 Processing batch {epoch_id} with {batch_df.count()} records")
        
        if batch_df.count() > 0:
            # Show sample data
            print("Sample data:")
            batch_df.show(5, truncate=False)
            
            # Write to ClickHouse using JDBC
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
                        "raw_payload String, updated_at DateTime64(3)") \
                .mode("append") \
                .save()
            
            print(f"✅ Batch {epoch_id} written to ClickHouse successfully")
        else:
            print(f"⏭️  Batch {epoch_id} is empty, skipping")
            
    except Exception as e:
        print(f"❌ Error writing batch {epoch_id} to ClickHouse: {e}")
        import traceback
        traceback.print_exc()

# Method 1: Stream from PostgreSQL using JDBC with polling
# Note: Spark doesn't have native PostgreSQL streaming, so we simulate it
# by reading in micro-batches based on updated_at timestamp

print("\n" + "=" * 80)
print("Starting streaming query...")
print("=" * 80)
print("\nThis will poll PostgreSQL every {} seconds for new engagement events".format(STREAMING_INTERVAL))
print("Press Ctrl+C to stop\n")

# For true streaming, you would typically use Kafka or another streaming source
# Here we simulate streaming by reading PostgreSQL in micro-batches

# Read content table once (static reference data)
df_content = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
    .option("dbtable", "content") \
    .option("user", "postgres") \
    .option("password", "mysecretpassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Create a streaming DataFrame that reads from PostgreSQL in micro-batches
# We'll use a custom source or read in a loop with timestamps

# Alternative approach: Use Spark's rate source for testing, then switch to real source
# For production, integrate with Kafka, Kinesis, or use PostgreSQL Change Data Capture (CDC)

# Method 2: Use Structured Streaming with a custom source or rate source for demo
# In production, you'd connect to Kafka or use Debezium for PostgreSQL CDC

print("\n⚠️  Note: For true event streaming, consider:")
print("  1. Using Kafka as an intermediate streaming source")
print("  2. Using PostgreSQL Change Data Capture (CDC) with Debezium")
print("  3. Using ClickHouse Kafka engine for direct ingestion")
print("\nFor now, using micro-batch polling approach...\n")

# Micro-batch approach: Read new events periodically
from datetime import datetime, timedelta
import time

last_timestamp = datetime.utcnow() - timedelta(minutes=5)  # Start from 5 minutes ago

try:
    while True:
        current_time = datetime.utcnow()
        
        # Read new events since last check
        query = f"""
            (SELECT id, content_id::text, user_id::text, event_type, 
                    event_ts, duration_ms, device, raw_payload::text, updated_at
             FROM engagement_events 
             WHERE updated_at > '{last_timestamp.isoformat()}'
             ORDER BY updated_at) AS new_events
        """
        
        df_new_events = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/postgres") \
            .option("dbtable", query) \
            .option("user", "postgres") \
            .option("password", "mysecretpassword") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        count = df_new_events.count()
        
        if count > 0:
            print(f"\n📥 Found {count} new events since {last_timestamp}")
            
            # Join with content for enrichment
            df_enriched = df_new_events.join(
                df_content,
                df_new_events.content_id == df_content.id.cast("string"),
                "left"
            ).select(
                df_new_events.id,
                df_new_events.content_id,
                df_new_events.user_id,
                df_new_events.event_type,
                df_new_events.event_ts,
                df_new_events.duration_ms,
                df_new_events.device,
                df_new_events.raw_payload,
                df_new_events.updated_at,
                df_content.slug.alias("content_slug"),
                df_content.title.alias("content_title"),
                df_content.content_type
            )
            
            # Write to ClickHouse
            df_enriched.write \
                .format("jdbc") \
                .option("url", clickhouse_url) \
                .option("dbtable", "engagement_events_stream") \
                .option("user", CLICKHOUSE_USER) \
                .option("password", CLICKHOUSE_PASSWORD) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .option("createTableColumnTypes",
                        "id Int64, content_id String, user_id String, event_type String, "
                        "event_ts DateTime64(3), duration_ms Int32, device String, "
                        "raw_payload String, updated_at DateTime64(3), "
                        "content_slug String, content_title String, content_type String") \
                .mode("append") \
                .save()
            
            print(f"✅ Written {count} events to ClickHouse")
            
            # Update timestamp for next iteration
            last_timestamp = current_time
        else:
            print(f"⏳ No new events (checked at {current_time.strftime('%H:%M:%S')})")
        
        # Wait for next interval
        time.sleep(STREAMING_INTERVAL)
        
except KeyboardInterrupt:
    print("\n\n🛑 Streaming stopped by user")
except Exception as e:
    print(f"\n❌ Error in streaming: {e}")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()
    print("Spark session closed")
