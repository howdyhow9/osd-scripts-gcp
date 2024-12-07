from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
from google.cloud import storage
import sys

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

def create_kafka_to_hudi_processor(spark_session, db_name, table_name):
    """Create a processor function with access to spark session and table info"""

    def kafka_to_hudi(df, batch_id):
        """Process each batch of Kafka data and write to Hudi"""

        print(f"Processing batch: {batch_id}")
        table_path = f"gs://osd-data/{db_name}/{table_name}"

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

            # Configure Hudi write options
            hudi_options = {
                'hoodie.table.name': table_name,
                'hoodie.datasource.write.recordkey.field': 'uuid',
                'hoodie.datasource.write.partitionpath.field': 'date',
                'hoodie.datasource.write.precombine.field': 'ts',
                'hoodie.datasource.write.operation': 'upsert',
                'hoodie.upsert.shuffle.parallelism': '2',
                'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
                'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
                'hoodie.cleaner.commits.retained': '10'
            }

            # Write batch to Hudi
            print(f"Writing to Hudi table at: {table_path}")
            final_df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save(table_path)

            print(f"Successfully wrote batch {batch_id} to {table_path}")

            # Print batch statistics
            print("Batch statistics:")
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
            raise

    return kafka_to_hudi

def verify_hudi_data(spark, table_path):
    """Verify Hudi table data"""
    try:
        # Read from Hudi table
        df = spark.read.format("hudi").load(table_path)

        # Calculate statistics
        stats = df.select(
            F.count("*").alias("total_records"),
            F.countDistinct("uuid").alias("unique_devices"),
            F.round(F.avg("consumption"), 2).alias("avg_consumption"),
            F.min("ts").alias("earliest_event"),
            F.max("ts").alias("latest_event"),
            F.min("processing_time").alias("first_processed"),
            F.max("processing_time").alias("last_processed")
        )

        print("Table statistics:")
        stats.show(truncate=False)

    except Exception as e:
        print(f"Error verifying Hudi data: {str(e)}")

def main():
    try:
        # Initialize Spark session
        print("Creating Spark session...")
        spark = create_spark_session()

        # Define schema and table names
        db_name = "kafka_hudi"
        table_name = "iot_events"
        table_path = f"gs://osd-data/{db_name}/{table_name}"

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
        # Create processor function with spark session and table info
        processor = create_kafka_to_hudi_processor(spark, db_name, table_name)

        # Write stream using foreachBatch
        query = df_kafka.writeStream \
            .foreachBatch(processor) \
            .outputMode("append") \
            .option("checkpointLocation", f"gs://osd-data/checkpoints/{db_name}/{table_name}") \
            .trigger(once=True) \
            .start()

        # Wait for the streaming to finish
        query.awaitTermination()

        print("Stream processing completed. Verifying results...")
        verify_hudi_data(spark, table_path)

    except Exception as e:
        print(f"Error in main process: {str(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()
            print("Spark session stopped")

if __name__ == "__main__":
    main()