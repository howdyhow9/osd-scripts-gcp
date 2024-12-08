from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *
import os
from google.cloud import storage
import sys
import time

# Set up GCS client and download the file
client = storage.Client()
bucket = client.get_bucket("osd-scripts")
blob = bucket.blob("spark_config_delta.py")
blob.download_to_filename("/tmp/spark_config_delta.py")

# Add the directory to system path
sys.path.insert(0, '/tmp')

# Import spark session creation module
from spark_config_delta import create_spark_session

def parse_json_value(df):
    """Parse the JSON value column into individual fields"""

    # Define schema to match exactly with the producer schema
    json_schema = StructType([
        StructField("uuid", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("consumption", DoubleType(), False),
        StructField("month", StringType(), False),
        StructField("day", StringType(), False),
        StructField("hour", StringType(), False),
        StructField("minute", StringType(), False),
        StructField("date", StringType(), False),
        StructField("key", StringType(), False)
    ])

    # Parse JSON value column, maintaining exact field names from producer
    parsed_df = df.withColumn("parsed_value",
                              F.from_json(F.col("value"), json_schema)) \
        .select(
        F.col("key").alias("kafka_key"),
        F.col("timestamp").alias("kafka_timestamp"),
        "parsed_value.*"
    )

    return parsed_df

def create_kafka_to_delta_processor(spark_session, db_name, table_name):
    """Create a processor function with access to spark session and table info"""

    def kafka_to_delta(df, batch_id):
        """Process each batch of Kafka data and write to Delta Lake"""
        if df.rdd.isEmpty():
            print(f"Batch {batch_id} is empty, skipping processing")
            return

        print(f"Processing batch: {batch_id}")
        table_loc = f"gs://osd-data/{db_name}.db/{table_name}"

        try:
            # Cast Kafka message format
            kafka_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

            # Parse JSON values
            parsed_df = parse_json_value(kafka_df)

            # Add processing metadata
            final_df = parsed_df.withColumn("processing_time", F.current_timestamp()) \
                .withColumn("batch_id", F.lit(batch_id))

            print("Preview of parsed data:")
            final_df.show(2, truncate=False)

            # Write batch to Delta Lake
            final_df.write \
                .format("delta") \
                .option("mergeSchema", "true") \
                .mode("append") \
                .save(table_loc)

            # Create or update table metadata
            spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            spark_session.sql(f"""
                CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
                USING delta
                LOCATION '{table_loc}'
            """)

            # Print batch statistics
            print(f"Batch {batch_id} statistics:")
            final_df.select(
                F.count("*").alias("total_records"),
                F.countDistinct("uuid").alias("unique_devices"),
                F.round(F.avg("consumption"), 2).alias("avg_consumption"),
                F.date_format(F.min("ts"), "yyyy-MM-dd HH:mm:ss").alias("earliest_event"),
                F.date_format(F.max("ts"), "yyyy-MM-dd HH:mm:ss").alias("latest_event")
            ).show(truncate=False)

        except Exception as e:
            print(f"Error processing batch {batch_id}: {str(e)}")
            import traceback
            print(f"Stack trace:\n{traceback.format_exc()}")
            # Don't raise the exception - let it continue processing next batch

    return kafka_to_delta

def monitor_streaming_query(query, db_name, table_name, spark):
    """Monitor the streaming query and periodically show statistics"""
    while query.isActive:
        try:
            # Print query progress
            print("\nStreaming Query Status:")
            print(f"Input rate: {query.lastProgress['inputRate'] if query.lastProgress else 'N/A'} records/sec")
            print(f"Processing rate: {query.lastProgress['processedRowsPerSecond'] if query.lastProgress else 'N/A'} records/sec")

            # Show table statistics every minute
            result = spark.sql(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT uuid) as unique_devices,
                    ROUND(AVG(consumption), 2) as avg_consumption,
                    MAX(ts) as latest_event,
                    MAX(processing_time) as last_processed
                FROM {db_name}.{table_name}
                WHERE processing_time >= current_timestamp() - INTERVAL 1 MINUTE
            """)
            print("\nLast minute statistics:")
            result.show(truncate=False)

        except Exception as e:
            print(f"Error in monitoring: {str(e)}")

        time.sleep(60)  

def main():
    # Initialize Spark session
    print("Creating Spark session...")
    spark = create_spark_session()

    # Set Delta configurations
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # Define schema and table names
    db_name = "kafka_delta"
    table_name = "iot_events"

    while True:  # Continuous retry loop
        try:
            print("Setting up Kafka stream...")
            # Read from Kafka stream
            df_kafka = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "osds-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092") \
                .option("failOnDataLoss", "false") \
                .option("subscribe", "osds-topic") \
                .option("startingOffsets", "latest") \
            .load()

        print("Starting continuous stream processing...")
        # Create processor function with spark session and table info
        processor = create_kafka_to_delta_processor(spark, db_name, table_name)

        # Write stream using foreachBatch
        query = df_kafka.writeStream \
            .foreachBatch(processor) \
            .outputMode("append") \
            .option("checkpointLocation", f"gs://osd-data/checkpoints/{db_name}/{table_name}") \
            .trigger(processingTime='1 minute') \
        .start()

    # Start the monitoring thread
    import threading
    monitor_thread = threading.Thread(
        target=monitor_streaming_query,
        args=(query, db_name, table_name, spark),
        daemon=True
    )
    monitor_thread.start()

    # Wait for the streaming to terminate (if it does)
    query.awaitTermination()

except Exception as e:
print(f"Error in stream processing: {str(e)}")
print("Attempting to restart streaming in 10 seconds...")
time.sleep(10)
continue

if __name__ == "__main__":
    main()