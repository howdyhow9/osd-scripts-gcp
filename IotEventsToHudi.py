from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
from google.cloud import storage
import sys
import time
from datetime import datetime
import threading

# Set up GCS client and download the file
client = storage.Client()
bucket = client.get_bucket("osd-scripts")
blob = bucket.blob("spark_config_hudi.py")
blob.download_to_filename("/tmp/spark_config_hudi.py")

# Add the directory to system path
sys.path.insert(0, '/tmp')

# Import spark session creation module
from spark_config_hudi import create_spark_session

def parse_json_value(df):
    """Parse the JSON value column into individual fields"""
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

    return df.withColumn("parsed_value",
                         F.from_json(F.col("value"), json_schema)) \
        .select(
        F.col("key").alias("kafka_key"),
        F.col("timestamp").alias("kafka_timestamp"),
        "parsed_value.*"
    )

def create_kafka_to_hudi_processor(spark_session, iDBSchema, iTable):
    """Create a processor function with access to spark session and table info"""

    def kafka_to_hudi(df, batch_id):
        """Process each batch of Kafka data and write to Hudi"""
        if df.rdd.isEmpty():
            print(f"Batch {batch_id} is empty, skipping processing")
            return

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\nProcessing batch {batch_id} at {current_time}")
        table_path = f"gs://osd-data/{iDBSchema}.db/{iTable}"

        try:
            # Create schema if not exists
            spark_session.sql(f"create database if not exists {iDBSchema}")

            # Cast Kafka message format
            kafka_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
            records_count = kafka_df.count()
            print(f"Number of records in Kafka batch: {records_count}")

            # Parse JSON values
            parsed_df = parse_json_value(kafka_df)

            # Add processing metadata
            final_df = parsed_df.withColumn("processing_time", F.current_timestamp()) \
                .withColumn("batch_id", F.lit(batch_id))

            # Configure Hudi write options
            hudiOptions = {
                'hoodie.table.name': f"{iDBSchema}_{iTable}",
                'hoodie.datasource.write.recordkey.field': 'uuid',
                'hoodie.datasource.write.precombine.field': 'ts',
                'hoodie.datasource.write.operation': 'upsert',
                'hoodie.upsert.shuffle.parallelism': '2',
                'hoodie.insert.shuffle.parallelism': '2',
                'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
                'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
                'hoodie.cleaner.commits.retained': '10',
                'hoodie.keep.min.commits': '20',
                'hoodie.keep.max.commits': '30',
                'hoodie.streaming.async.compact': 'true',
                'hoodie.compact.inline': 'false',
                'hoodie.compact.inline.max.delta.commits': '20'
            }

            # Write to Hudi table
            print(f"Writing batch to Hudi table at: {table_path}")
            final_df.write \
                .format("org.apache.hudi") \
                .options(**hudiOptions) \
                .mode("append") \
                .save(table_path)

            # Register table in Hive metastore if not exists
            spark_session.sql(f"""
                CREATE TABLE IF NOT EXISTS {iDBSchema}.{iTable}
                USING hudi
                LOCATION '{table_path}'
            """)

            # Print batch statistics
            stats = final_df.select(
                F.count("*").alias("total_records"),
                F.countDistinct("uuid").alias("unique_devices"),
                F.round(F.avg("consumption"), 2).alias("avg_consumption"),
                F.date_format(F.min("ts"), "yyyy-MM-dd HH:mm:ss").alias("earliest_event"),
                F.date_format(F.max("ts"), "yyyy-MM-dd HH:mm:ss").alias("latest_event")
            ).collect()[0]

            print("\nBatch Statistics:")
            print(f"Records processed: {stats['total_records']}")
            print(f"Unique devices: {stats['unique_devices']}")
            print(f"Average consumption: {stats['avg_consumption']}")
            print(f"Time range: {stats['earliest_event']} to {stats['latest_event']}")

        except Exception as e:
            print(f"Error processing batch {batch_id}: {str(e)}")
            import traceback
            print(f"Stack trace:\n{traceback.format_exc()}")
            # Don't raise the exception to keep the stream running

    return kafka_to_hudi

def monitor_streaming_query(spark, query, iDBSchema, iTable):
    """Monitor the streaming query and show periodic statistics"""
    while query.isActive:
        try:
            time.sleep(60)  # Wait for 1 minute between stats

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\nStreaming Statistics at {current_time}")

            # Show query progress
            if query.lastProgress:
                print(f"Input rate: {query.lastProgress['inputRate']} records/sec")
                print(f"Processing rate: {query.lastProgress['processedRowsPerSecond']} records/sec")

            # Show recent data statistics
            result = spark.sql(f"""
                SELECT 
                    COUNT(*) as records_last_minute,
                    COUNT(DISTINCT uuid) as unique_devices,
                    ROUND(AVG(consumption), 2) as avg_consumption,
                    MAX(ts) as latest_event
                FROM {iDBSchema}.{iTable}
                WHERE processing_time >= current_timestamp() - INTERVAL 1 MINUTE
            """)
            print("\nLast minute statistics:")
            result.show(truncate=False)

            # Show table info
            print("\nHudi table information:")
            read_df = spark.read.format("hudi").load(f"gs://osd-data/{iDBSchema}.db/{iTable}")
            read_df.select(
                F.count("*").alias("total_records"),
                F.countDistinct("uuid").alias("unique_devices"),
                F.round(F.avg("consumption"), 2).alias("avg_consumption"),
                F.max("processing_time").alias("latest_processing_time")
            ).show(truncate=False)

        except Exception as e:
            print(f"Error in monitoring: {str(e)}")

def main():
    print("Creating Spark session...")
    spark = create_spark_session()

    # Define schema and table names
    iDBSchema = "kafka_hudi"
    iTable = "iot_events"

    while True:  # Continuous retry loop
        try:
            print("\nSetting up Kafka stream...")
            df_kafka = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "osds-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092") \
                .option("subscribe", "osds-topic") \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()

            print("Starting continuous stream processing...")
            processor = create_kafka_to_hudi_processor(spark, iDBSchema, iTable)

            query = df_kafka.writeStream \
                .foreachBatch(processor) \
                .outputMode("append") \
                .option("checkpointLocation", f"gs://osd-data/checkpoints/{iDBSchema}.db/{iTable}") \
                .trigger(processingTime='1 minute') \
                .start()

            # Start monitoring thread
            monitor_thread = threading.Thread(
                target=monitor_streaming_query,
                args=(spark, query, iDBSchema, iTable),
                daemon=True
            )
            monitor_thread.start()

            print("\nStream processing started successfully")
            print("Monitoring for new data...")

            # Wait for the streaming to terminate (if it does)
            query.awaitTermination()

        except Exception as e:
            print(f"\nError in stream processing: {str(e)}")
            print("Waiting 10 seconds before restarting stream...")
            time.sleep(10)
            continue

if __name__ == "__main__":
    main()