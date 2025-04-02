# ride-sharing-spark-streaming
Spark structured streaming example

# Ride-Sharing Analytics Streaming Tasks with Apache Spark

This project demonstrates how to use Apache Spark Structured Streaming to process ride-sharing data in real time. It covers three tasks:

- **Task 1:** Basic Streaming Ingestion and JSON Parsing
- **Task 2:** Real-Time Aggregations (Driver-Level)
- **Task 3:** Windowed Time-Based Analytics

Each task includes detailed code explanations, sample outputs, and instructions for executing the application.

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

## Task 2: Real-Time Aggregations (Driver-Level)

### Overview (Task 2)
This task computes real-time aggregations from streaming ride-sharing data:
- **Total fare amount** (using `SUM(fare_amount)`)
- **Average distance** (using `AVG(distance_km)`)

The aggregated results are both displayed on the console and written to CSV files using a `foreachBatch` sink.

### Code Explanation (Task 2)
1. **Reuse Parsed Data:**  
   The data is parsed from JSON (using a predefined schema) from a socket source.
2. **Aggregation:**  
   Group the data by `driver_id` and calculate:
   - `total_fare` (sum of `fare_amount`)
   - `avg_distance` (average of `distance_km`)
3. **Console Output:**  
   The aggregation is output in `complete` mode to the console.
4. **CSV Output via foreachBatch:**  
   Each microbatch is written as CSV (in append mode) to a specified output directory. A unique checkpoint directory is used for maintaining state.

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

# Task 3: Windowed Time-Based Analytics

This task performs a windowed aggregation on the streaming ride-sharing data. Specifically, it:

- Converts the incoming timestamp (initially a string) into a proper TimestampType.
- Applies a watermark and performs a 5-minute windowed aggregation (sliding every 1 minute) to compute the sum of `fare_amount`.
- Flattens the window column (extracting `window.start` and `window.end`) to enable CSV output.
- Writes the aggregated results to CSV files.

---

## Code Explanation

1. **Spark Session Creation:**  
   Initializes the Spark application and configures logging.

2. **Schema Definition:**  
   Defines the expected schema for the incoming JSON data. Here, the `timestamp` field is initially a string.

3. **Socket Stream Reading:**  
   Reads streaming data from a socket (e.g., `localhost:9999`).

4. **JSON Parsing and Timestamp Conversion:**  
   Parses the JSON data using the defined schema and converts the string-based timestamp to a proper `TimestampType` with `to_timestamp`.

5. **Windowed Aggregation:**  
   - Applies a watermark of 5 minutes to the event time.
   - Uses a sliding window of 5 minutes that slides every 1 minute to aggregate the sum of `fare_amount`.

6. **Flattening the Window Column:**  
   Extracts `window.start` and `window.end` into separate columns because CSV does not support complex types.

7. **CSV Output:**  
   Writes the flattened aggregation result to CSV files using append mode. A unique checkpoint directory is specified for state management.

---

## Code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, window, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("WindowedFareAnalytics") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 2. Define schema (timestamp as string to demonstrate conversion)
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

# 3. Read from socket
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 4. Parse JSON and convert timestamp to TimestampType
parsed_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")
parsed_stream = parsed_stream.withColumn("event_time", to_timestamp(col("timestamp")))

# 5. Perform a 5-minute windowed aggregation (sliding every 1 minute) on fare_amount
windowed_agg = parsed_stream \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(window(col("event_time"), "5 minutes", "1 minute")) \
    .agg(_sum("fare_amount").alias("total_fare"))

# 6. Flatten the window column (extract window.start and window.end)
flattened = windowed_agg.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare")
)

# 7. Write the windowed results to CSV
query = flattened.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/task3_windowed/") \
    .option("checkpointLocation", "output/task3_checkpoint/") \
    .start()

query.awaitTermination()

