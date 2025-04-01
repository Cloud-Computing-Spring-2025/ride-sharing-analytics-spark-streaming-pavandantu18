from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, window, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("WindowedFareAnalytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Define schema (timestamp as string)
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())  # string initially

# 3. Read from socket
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 4. Parse JSON and convert timestamp
parsed_stream = raw_stream \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp")))

# 5. Windowed aggregation (5-min window sliding every 1 min)
windowed_agg = parsed_stream \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(window(col("event_time"), "5 minutes", "1 minute")) \
    .agg(_sum("fare_amount").alias("total_fare"))

# 6. Flatten window column
flattened = windowed_agg \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_fare")
    )

# 7. Write to CSV
query = flattened.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/task3_windowed/") \
    .option("checkpointLocation", "output/task3_checkpoint/") \
    .start()

query.awaitTermination()
