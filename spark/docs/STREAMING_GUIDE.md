# Spark Streaming to ClickHouse Guide

## ✅ Yes, Apache Spark Streaming Works with ClickHouse!

Apache Spark **Structured Streaming** is fully compatible with ClickHouse Cloud. You can stream events in real-time and write them directly to ClickHouse.

## 🎯 Streaming Approaches

### 1. **Micro-Batch Polling** (PostgreSQL → ClickHouse)
- **Script**: `postgres_to_clickhouse_streaming.py`
- **How it works**: Polls PostgreSQL periodically for new events
- **Best for**: Simple setups, low latency not critical
- **Latency**: Depends on polling interval (default: 10 seconds)

### 2. **Kafka Streaming** (Kafka → ClickHouse) ⭐ Recommended
- **Script**: `kafka_to_clickhouse_streaming.py`
- **How it works**: True event streaming from Kafka topics
- **Best for**: Production, real-time requirements
- **Latency**: Sub-second to seconds (configurable)

### 3. **Batch Processing** (Current)
- **Script**: `postgres_to_clickhouse.py`
- **How it works**: One-time batch processing
- **Best for**: Historical data, scheduled jobs

## 🚀 Quick Start: Micro-Batch Streaming

### Step 1: Configure ClickHouse

```bash
export CLICKHOUSE_HOST=your-host.clickhouse.cloud
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=your_password
export STREAMING_INTERVAL=10  # seconds
```

### Step 2: Run Streaming Script

```bash
docker exec -it spark_master bash -c "
  export CLICKHOUSE_HOST=your-host.clickhouse.cloud
  export CLICKHOUSE_USER=default
  export CLICKHOUSE_PASSWORD=your_password
  export STREAMING_INTERVAL=10
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.7.1,com.clickhouse:clickhouse-jdbc:0.8.0 \
    /opt/spark/scripts/postgres_to_clickhouse_streaming.py
"
```

The script will:
- Poll PostgreSQL every 10 seconds for new events
- Process and enrich events with content metadata
- Write to ClickHouse table `engagement_events_stream`
- Continue running until stopped (Ctrl+C)

## 🔥 Advanced: Kafka Streaming Setup

For true event streaming, add Kafka to your setup:

### Add Kafka to compose.yaml

```yaml
kafka:
  image: confluentinc/cp-kafka:latest
  container_name: kafka
  environment:
    KAFKA_BROKER_ID: 1
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  ports:
    - "9092:9092"
  depends_on:
    - zookeeper
  networks:
    - spark-network

zookeeper:
  image: confluentinc/cp-zookeeper:latest
  container_name: zookeeper
  environment:
    ZOOKEEPER_CLIENT_PORT: 2181
  networks:
    - spark-network
```

### Run Kafka Streaming

```bash
docker exec -it spark_master bash -c "
  export KAFKA_BOOTSTRAP_SERVERS=kafka:9092
  export KAFKA_TOPIC=engagement_events
  export CLICKHOUSE_HOST=your-host.clickhouse.cloud
  export CLICKHOUSE_USER=default
  export CLICKHOUSE_PASSWORD=your_password
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.clickhouse:clickhouse-jdbc:0.8.0 \
    /opt/spark/scripts/kafka_to_clickhouse_streaming.py
"
```

## 📊 How It Works

### Spark Structured Streaming Architecture

```
Event Source → Spark Streaming → Processing → ClickHouse
   (Kafka/     (Micro-batches)   (Transform/    (Analytics
   PostgreSQL)                    Aggregate)     Storage)
```

### Key Concepts

1. **Micro-Batches**: Spark processes data in small batches (configurable interval)
2. **Checkpointing**: Spark maintains state and handles failures automatically
3. **Exactly-Once Semantics**: Ensures no data loss or duplication
4. **Windowing**: Supports time-based aggregations (e.g., 5-minute windows)

### Processing Flow

1. **Read**: Stream from source (Kafka/PostgreSQL)
2. **Transform**: Apply business logic, enrich data
3. **Aggregate**: Calculate metrics, windowing
4. **Write**: Write to ClickHouse using `foreachBatch`

## 🔧 Configuration Options

### Streaming Interval

Control how often Spark processes batches:

```python
# In the script
STREAMING_INTERVAL = 10  # seconds

# Or via environment variable
export STREAMING_INTERVAL=5  # 5-second batches
```

### ClickHouse Write Modes

- **`append`**: Add new rows (default for streaming)
- **`overwrite`**: Replace table (use with caution)
- **`upsert`**: Update or insert (requires custom logic)

### Checkpoint Location

Spark maintains checkpoints for fault tolerance:

```python
.config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
```

## 📈 Performance Considerations

### Batch Size

- **Smaller batches** (1-5 seconds): Lower latency, more overhead
- **Larger batches** (10-60 seconds): Better throughput, higher latency

### ClickHouse Optimization

1. **Batch Inserts**: Write multiple rows per batch
2. **Table Engine**: Use `MergeTree` for time-series data
3. **Partitioning**: Partition by date for better performance
4. **Ordering Key**: Order by timestamp for fast queries

### Example ClickHouse Table

```sql
CREATE TABLE engagement_events_stream (
    id Int64,
    content_id String,
    user_id String,
    event_type String,
    event_ts DateTime64(3),
    duration_ms Int32,
    device String,
    raw_payload String,
    processed_at DateTime64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_ts)
ORDER BY (event_ts, content_id);
```

## 🛠️ Troubleshooting

### Issue: No events being processed

**Check:**
- Is the source producing data?
- Are credentials correct?
- Check Spark logs: `docker logs spark_master`

### Issue: ClickHouse connection errors

**Check:**
- ClickHouse Cloud host is accessible
- SSL is enabled (`?ssl=true`)
- Credentials are correct
- Firewall allows connections

### Issue: High latency

**Solutions:**
- Reduce streaming interval
- Increase Spark worker resources
- Optimize ClickHouse table structure
- Use Kafka for better throughput

## 📚 Additional Resources

- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [ClickHouse JDBC Driver](https://github.com/ClickHouse/clickhouse-java)
- [Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)

## 🎯 Best Practices

1. **Use Kafka** for production event streaming
2. **Set appropriate intervals** based on latency requirements
3. **Monitor checkpoint locations** for disk space
4. **Use windowing** for time-based aggregations
5. **Handle failures** gracefully with retry logic
6. **Optimize ClickHouse tables** for your query patterns

## 🔄 Comparison: Batch vs Streaming

| Feature | Batch | Streaming |
|---------|-------|-----------|
| **Latency** | Minutes to hours | Seconds to minutes |
| **Use Case** | Historical data | Real-time analytics |
| **Complexity** | Simple | More complex |
| **Resource Usage** | High (one-time) | Continuous |
| **Fault Tolerance** | Manual retry | Automatic recovery |

Choose **batch** for:
- Historical data processing
- Scheduled reports
- One-time migrations

Choose **streaming** for:
- Real-time dashboards
- Live monitoring
- Event-driven applications
