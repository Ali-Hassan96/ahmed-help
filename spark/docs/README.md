# Spark Scripts

This directory contains Spark scripts for processing data from PostgreSQL, aggregating it, and sending it to ClickHouse Cloud.

## 📚 Available Scripts

- **`postgres_to_clickhouse.py`** - Batch processing (one-time ETL)
- **`postgres_to_clickhouse_streaming.py`** - Micro-batch streaming (real-time)
- **`kafka_to_clickhouse_streaming.py`** - Kafka event streaming (production-ready)
- **`read_postgres.py`** - Sample PostgreSQL reader

## 🚀 Streaming Support

**Yes, Apache Spark Streaming works with ClickHouse!** 

See [STREAMING_GUIDE.md](./STREAMING_GUIDE.md) for complete streaming documentation.

### Quick Streaming Example

```bash
# Micro-batch streaming (polls PostgreSQL every 10 seconds)
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

## Running Spark Scripts

### PostgreSQL to ClickHouse Cloud

This script reads data from PostgreSQL, processes and aggregates it, then writes to ClickHouse Cloud.

**Before running, configure your ClickHouse Cloud credentials:**

```bash
# Set environment variables
export CLICKHOUSE_HOST=your-host.clickhouse.cloud
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=your_password
export CLICKHOUSE_DATABASE=default  # optional, defaults to 'default'
export CLICKHOUSE_PORT=8443  # optional, defaults to '8443'
```

**Run from outside the container:**

```bash
docker exec -it spark_master bash -c "
  export CLICKHOUSE_HOST=your-host.clickhouse.cloud
  export CLICKHOUSE_USER=default
  export CLICKHOUSE_PASSWORD=your_password
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.7.1,com.clickhouse:clickhouse-jdbc:0.8.0 \
    /opt/spark/scripts/postgres_to_clickhouse.py
"
```

**Or modify the script directly** to set the credentials in the script itself.

### Read from PostgreSQL (sample script)

```bash
# Read from PostgreSQL
docker exec -it spark_master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.1 \
  /opt/spark/scripts/read_postgres.py
```

### From inside the container:

```bash
docker exec -it spark_master bash
cd /opt/spark/scripts
export CLICKHOUSE_HOST=your-host.clickhouse.cloud
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=your_password
spark-submit --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.1,com.clickhouse:clickhouse-jdbc:0.8.0 \
  postgres_to_clickhouse.py
```

## Spark Web UI

Access the Spark Web UI at: http://localhost:8080

## Connection Details

- **PostgreSQL**: `postgres:5432` (from within Docker network)
- **Redis**: `redis:6379` (from within Docker network)
- **Spark Master**: `spark://spark-master:7077`
- **ClickHouse Cloud**: Configure via environment variables (see above)

## What the PostgreSQL to ClickHouse Script Does

1. **Reads** data from PostgreSQL tables:
   - `content` table
   - `engagement_events` table

2. **Processes and Aggregates**:
   - Joins content with engagement events
   - Calculates metrics per content:
     - Total events, play/pause/finish/click counts
     - Total and average duration
     - Unique users
     - Completion rate
   - Creates daily aggregations for time-series analysis

3. **Writes to ClickHouse Cloud**:
   - `content_engagement_metrics` - Overall metrics per content
   - `content_engagement_daily` - Daily metrics per content

## 📖 Documentation

- **[STREAMING_GUIDE.md](./STREAMING_GUIDE.md)** - Complete guide to Spark Streaming with ClickHouse
  - Micro-batch polling approach
  - Kafka streaming setup
  - Performance optimization
  - Troubleshooting

## Creating New Scripts

1. Add your Python script to this directory
2. It will be available at `/opt/spark/scripts/` inside the container
3. Run it using `spark-submit` as shown above

## 🔄 Batch vs Streaming

| Use Case | Script | When to Use |
|----------|--------|-------------|
| **Historical Data** | `postgres_to_clickhouse.py` | One-time processing, scheduled jobs |
| **Real-time Events** | `postgres_to_clickhouse_streaming.py` | Continuous processing, low latency needed |
| **Production Streaming** | `kafka_to_clickhouse_streaming.py` | Kafka-based event streaming |
