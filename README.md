# ride-sharing-spark-streaming
Spark structured streaming example

# Ride-Sharing Analytics Streaming Tasks with Apache Spark

This project demonstrates how to use Apache Spark Structured Streaming to process ride-sharing data in real time. It covers three tasks:

- **Task 1:** Basic Streaming Ingestion and JSON Parsing
- **Task 2:** Real-Time Aggregations (Driver-Level)
- **Task 3:** Windowed Time-Based Analytics

Each task includes detailed code explanations, sample outputs, and instructions for executing the application.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Task 1: Basic Streaming Ingestion and JSON Parsing](#task-1-basic-streaming-ingestion-and-json-parsing)
- [Task 2: Real-Time Aggregations (Driver-Level)](#task-2-real-time-aggregations-driver-level)
- [Task 3: Windowed Time-Based Analytics](#task-3-windowed-time-based-analytics)
- [Additional Notes & Troubleshooting](#additional-notes--troubleshooting)

---

## Prerequisites

- **Apache Spark** (version 2.4 or later recommended)
- **Python 3.x** with PySpark installed
- **Netcat (nc)** or a similar tool to simulate streaming data
- A terminal to run `spark-submit` and `nc`

---

## Task 1: Basic Streaming Ingestion and JSON Parsing

### Overview
This task ingests streaming JSON data from a socket (`localhost:9999`) and parses it into a structured Spark DataFrame. The parsed data is printed to the console.

### Code Explanation
1. **Spark Session Creation:**  
   Initializes the Spark application and sets the logging level.
2. **Schema Definition:**  
   Specifies the structure of the incoming JSON messages (columns: `trip_id`, `driver_id`, `distance_km`, `fare_amount`, `timestamp`).
3. **Socket Stream Reading:**  
   Reads streaming data from `localhost:9999` using `spark.readStream.format("socket")`.
4. **JSON Parsing:**  
   Uses `from_json()` to parse the raw JSON data into structured columns.
5. **Console Output:**  
   Writes the parsed data to the console in append mode.

### Code
```python
# task1.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("StreamingJSONParser") \
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

# 5. Output the parsed data to the console
query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

# task2.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, avg
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("DriverLevelAggregations") \
    .getOrCreate()
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

# 5. Aggregate data by driver_id
aggregated_stream = parsed_stream.groupBy("driver_id") \
    .agg(
        _sum("fare_amount").alias("total_fare"),
        avg("distance_km").alias("avg_distance")
    )

# 6. Write aggregated results to console in complete mode
console_query = aggregated_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# 7. Write aggregated results to CSV using foreachBatch
def write_to_csv(batch_df, batch_id):
    batch_df.write.mode("append").csv("output/task2")

csv_query = aggregated_stream.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", "output/task2_checkpoint/") \
    .start()

console_query.awaitTermination()
csv_query.awaitTermination()
