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
blob = bucket.blob("spark_config_iceberg.py")
blob.download_to_filename("/tmp/spark_config_iceberg.py")

# Add the directory to system path
sys.path.insert(0, '/tmp')

# Import spark session creation module
from spark_config_iceberg import create_spark_session

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

    parsed_df = df.withColumn("parsed_value",
                              F.from_json(F.col("value"), json_schema)) \
        .select(
        F.col("key").alias("kafka_key"),
        F.col("timestamp").alias("kafka_timestamp"),
        "parsed_value.*"
    )

    return parsed_df

def process_kafka_batch(spark, df, batch_id, db_schema, table_name):
    """Process a batch of Kafka data and write to Iceberg"""
    if df.rdd.isEmpty():
        print(f"Batch {batch_id} is empty, skipping processing")
        return

    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\nProcessing batch {batch_id} at {current_time}")

        # Get table location path
        table_path = f"gs://osd-data/{db_schema}.db/{table_name}"

        # Cast Kafka message format
        kafka_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

        # Parse JSON values
        parsed_df = parse_json_value(kafka_df)

        # Add processing metadata
        final_df = parsed_df.withColumn("processing_time", F.current_timestamp()) \
            .withColumn("batch_id", F.lit(batch_id))

        # Create schema if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_schema}")

        # Create table if it doesn't exist
        table_exists = (spark.sql(f"SHOW TABLES IN {db_schema}")
                        .filter(F.col("tableName") == table_name)
                        .count() > 0)

        if not table_exists:
            create_table_sql = f"""
            CREATE TABLE {db_schema}.{table_name} (
                {', '.join([f"{col} {str(dtype).replace('Type','')}"
                            for col, dtype in final_df.dtypes])}
            )
            USING iceberg
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy',
                'write.object-storage.enabled' = 'true',
                'write.data.path' = '{table_path}/data',
                'write.metadata.path' = '{table_path}/metadata',
                'format-version' = '2',
                'write.metadata.delete-after-commit.enabled' = 'true',
                'write.metadata.previous-versions-max' = '5'
            )
            """
            spark.sql(create_table_sql)
            print(f"Created new table {db_schema}.{table_name}")

        # Write batch to Iceberg
        final_df.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable(f"{db_schema}.{table_name}")

        # Print batch statistics
        print("\nBatch Statistics:")
        stats = final_df.select(
            F.count("*").alias("total_records"),
            F.countDistinct("uuid").alias("unique_devices"),
            F.round(F.avg("consumption"), 2).alias("avg_consumption"),
            F.date_format(F.min("ts"), "yyyy-MM-dd HH:mm:ss").alias("earliest_event"),
            F.date_format(F.max("ts"), "yyyy-MM-dd HH:mm:ss").alias("latest_event")
        ).collect()[0]

        print(f"Records processed: {stats['total_records']}")
        print(f"Unique devices: {stats['unique_devices']}")
        print(f"Average consumption: {stats['avg_consumption']}")
        print(f"Time range: {stats['earliest_event']} to {stats['latest_event']}")

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")
        # Don't raise the exception to keep the stream running

def monitor_streaming_query(spark, query, db_schema, table_name):
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
                FROM {db_schema}.{table_name}
                WHERE processing_time >= current_timestamp() - INTERVAL 1 MINUTE
            """)
            print("\nLast minute statistics:")
            result.show(truncate=False)

            # Show table history
            print("\nRecent table history:")
            spark.sql(f"SELECT * FROM {db_schema}.{table_name}.history LIMIT 5").show(truncate=False)

        except Exception as e:
            print(f"Error in monitoring: {str(e)}")

def main():
    print("Creating Spark session...")
    spark = create_spark_session()

    # Define schema and table names
    db_name = "kafka_iceberg"
    table_name = "iot_events"

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
            query = df_kafka.writeStream \
                .foreachBatch(lambda df, batch_id: process_kafka_batch(
                spark, df, batch_id, db_name, table_name
            )) \
                .outputMode("append") \
                .option("checkpointLocation", f"gs://osd-data/checkpoints/{db_name}/{table_name}") \
                .trigger(processingTime='1 minute') \
                .start()

            # Start monitoring thread
            monitor_thread = threading.Thread(
                target=monitor_streaming_query,
                args=(spark, query, db_name, table_name),
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