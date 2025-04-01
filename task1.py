from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# 1. Create a Spark session
spark = SparkSession.builder \
    .appName("StreamingJSONToCSV") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Define the schema of the JSON messages
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", TimestampType())

# 3. Read data from the socket stream
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 4. Parse the incoming JSON strings using the schema
parsed_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

# 5. Output the parsed data to CSV files
query = parsed_stream.writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", "output/task1") \
    .option("checkpointLocation", "parsed_output_csv/checkpoint/") \
    .start()

query.awaitTermination()
