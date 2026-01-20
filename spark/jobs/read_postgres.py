"""
Sample Spark script to read data from PostgreSQL
Run with: docker exec -it spark_master spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.7.1 /opt/spark/scripts/read_postgres.py
"""

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("PostgreSQLReader") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

# Read from PostgreSQL
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
    .option("dbtable", "content") \
    .option("user", "postgres") \
    .option("password", "mysecretpassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Show the data
print("Content table:")
df.show()

# Read engagement_events
df_engagement = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
    .option("dbtable", "engagement_events") \
    .option("user", "postgres") \
    .option("password", "mysecretpassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

print("\nEngagement events:")
df_engagement.show()

# Perform some analysis
print("\nContent by type:")
df.groupBy("content_type").count().show()

print("\nEngagement events by type:")
df_engagement.groupBy("event_type").count().show()

spark.stop()
