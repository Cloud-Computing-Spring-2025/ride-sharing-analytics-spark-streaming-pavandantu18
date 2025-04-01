from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, avg
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# 1. Create Spark session
spark = SparkSession.builder.appName("DriverLevelAggregations").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 2. Define the schema
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", TimestampType())

# 3. Read from the socket stream
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 4. Parse JSON messages into a structured DataFrame
parsed_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

# 5. Aggregate data by driver_id (compute total fare and average distance)
aggregated_stream = parsed_stream.groupBy("driver_id") \
    .agg(
        _sum("fare_amount").alias("total_fare"),
        avg("distance_km").alias("avg_distance")
    )

# 6. Write to CSV using foreachBatch
def write_to_csv(batch_df, batch_id):
    # Write the static batch to CSV in append mode.
    batch_df.write.mode("append").csv("output/task2")

csv_query = aggregated_stream.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", "output/task2_checkpoint/") \
    .start()

csv_query.awaitTermination()
