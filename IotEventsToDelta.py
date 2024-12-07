from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *
import os
from google.cloud import storage
import sys
import json

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

    # Define schema for the JSON data
    json_schema = StructType([
        StructField("uuid", StringType(), True),
        StructField("ts", TimestampType(), True),
        StructField("consumption", DoubleType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True),
        StructField("hour", StringType(), True),
        StructField("minute", StringType(), True),
        StructField("date", StringType(), True),
        StructField("key", StringType(), True)
    ])

    # Parse JSON value column and rename duplicated key column
    return df.withColumn("parsed_value",
                         F.from_json(F.col("value"), json_schema)) \
        .select(
        F.col("key").alias("kafka_key"),
        "timestamp",
        F.col("parsed_value.uuid"),
        F.col("parsed_value.ts"),
        F.col("parsed_value.consumption"),
        F.col("parsed_value.month"),
        F.col("parsed_value.day"),
        F.col("parsed_value.hour"),
        F.col("parsed_value.minute"),
        F.col("parsed_value.date"),
        F.col("parsed_value.key").alias("event_key")
    )

def initialize_delta_table(spark, table_loc):
    """Initialize or recreate Delta table with correct schema"""
    try:
        # Try to delete the existing table location
        dbutils = spark.conf.get("spark.databricks.service.dbutils", None)
        if dbutils:
            dbutils.fs.rm(table_loc, True)
        print(f"Cleaned up existing table at {table_loc}")
    except Exception as e:
        print(f"Note: Could not clean up table location: {str(e)}")

def kafka_to_delta(df, batch_id):
    """Process each batch of Kafka data and write to Delta Lake"""

    print(f"Processing batch: {batch_id}")
    iDBSchema = "kafka_delta"
    iTable = "kafka"
    table_loc = f"gs://osd-data/{iDBSchema}.db/{iTable}"

    try:
        # Cast Kafka message format
        kafka_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

        # Parse JSON values
        parsed_df = parse_json_value(kafka_df)

        # Add processing metadata
        final_df = parsed_df.withColumn("processing_time", F.current_timestamp()) \
            .withColumn("batch_id", F.lit(batch_id))

        print("Preview of parsed data:")
        final_df.show(5, truncate=False)
        print("Schema of parsed data:")
        final_df.printSchema()

        # Drop the table if it's the first batch
        if batch_id == 0:
            print("First batch - initializing table...")
            spark.sql(f"DROP TABLE IF EXISTS {iDBSchema}.{iTable}")
            initialize_delta_table(spark, table_loc)

        # Write batch to Delta Lake with schema merging enabled
        final_df.write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .mode("append") \
            .save(table_loc)

        # Create or update table metadata
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {iDBSchema}")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {iDBSchema}.{iTable}
            USING delta
            LOCATION '{table_loc}'
        """)

        print(f"Successfully wrote batch {batch_id} to {iDBSchema}.{iTable}")

        # Print some batch statistics
        print("Batch statistics:")
        final_df.select(
            F.count("*").alias("total_records"),
            F.countDistinct("uuid").alias("unique_devices"),
            F.round(F.avg("consumption"), 2).alias("avg_consumption")
        ).show()

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")
        raise

def main():
    try:
        # Initialize Spark session
        print("Creating Spark session...")
        spark = create_spark_session()

        # Set Delta configurations
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        # Define schema and table names
        iDBSchema = "kafka_delta"
        iTable = "kafka"

        # Drop existing table if any
        table_loc = f"gs://osd-data/{iDBSchema}.db/{iTable}"
        spark.sql(f"DROP TABLE IF EXISTS {iDBSchema}.{iTable}")
        initialize_delta_table(spark, table_loc)

        print("Setting up Kafka stream...")
        # Read from Kafka stream
        df_kafka = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "osds-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092") \
            .option("failOnDataLoss", "false") \
            .option("subscribe", "osds-topic") \
            .option("startingOffsets", "earliest") \
            .load()

        print("Starting stream processing...")
        # Write stream using foreachBatch
        query = df_kafka.writeStream \
            .foreachBatch(kafka_to_delta) \
            .outputMode("append") \
            .option("checkpointLocation", f"gs://osd-data/checkpoints/{iDBSchema}/{iTable}") \
            .trigger(once=True) \
            .start()

        # Wait for the streaming to finish
        query.awaitTermination()

        print("Stream processing completed. Verifying results...")
        # Verify the table after processing
        result = spark.sql(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT uuid) as unique_devices,
                ROUND(AVG(consumption), 2) as avg_consumption,
                MIN(processing_time) as first_processed,
                MAX(processing_time) as last_processed
            FROM {iDBSchema}.{iTable}
        """)
        print("Final table statistics:")
        result.show(truncate=False)

    except Exception as e:
        print(f"Error in main process: {str(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")
    finally:
        if 'spark' in locals():
            spark.stop()
            print("Spark session stopped")

if __name__ == "__main__":
    main()