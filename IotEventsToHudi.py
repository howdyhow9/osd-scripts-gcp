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

def create_kafka_to_hudi_processor(spark_session, iDBSchema, iTable):
    """Create a processor function with access to spark session and table info"""

    def kafka_to_hudi(df, batch_id):
        """Process each batch of Kafka data and write to Hudi"""

        print(f"Processing batch: {batch_id}")
        table_path = f"gs://osd-data/{iDBSchema}.db/{iTable}"

        try:
            # Create schema if not exists
            try:
                spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {iDBSchema}")
                print(f"Schema {iDBSchema} created or already exists")
            except Exception as e:
                print(f"Error creating schema {iDBSchema}: {str(e)}")
                raise

            # Drop and recreate the Hudi table definition
            spark_session.sql(f"DROP TABLE IF EXISTS {iDBSchema}.{iTable}")
            spark_session.sql(f"""
                CREATE TABLE {iDBSchema}.{iTable}
                USING hudi
                LOCATION '{table_path}'
                TBLPROPERTIES (
                    'hoodie.table.name'='{iDBSchema}_{iTable}',
                    'hoodie.datasource.write.recordkey.field'='uuid',
                    'hoodie.datasource.write.partitionpath.field'='date',
                    'hoodie.datasource.write.precombine.field'='ts',
                    'hoodie.datasource.write.operation'='bulk_insert',
                    'hoodie.bulkinsert.shuffle.parallelism'='2',
                    'hoodie.datasource.write.table.type'='COPY_ON_WRITE',
                    'hoodie.cleaner.policy'='KEEP_LATEST_COMMITS',
                    'hoodie.cleaner.commits.retained'='10',
                    'hoodie.keep.min.commits'='20',
                    'hoodie.keep.max.commits'='30'
                )
            """)
            print(f"Table {iDBSchema}.{iTable} created or recreated")

            # Cast Kafka message format
            kafka_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
            print("Number of records in Kafka batch:", df.count())

            # Parse JSON values
            parsed_df = parse_json_value(kafka_df)
            print("Number of records after parsing:", parsed_df.count())

            # Add processing metadata
            final_df = parsed_df.withColumn("processing_time", F.current_timestamp()) \
                .withColumn("batch_id", F.lit(batch_id))

            print("Preview of parsed data:")
            final_df.show(5, truncate=False)
            print("Schema of parsed data:")
            final_df.printSchema()

            # Configure Hudi write options following reference pattern
            hudiOptions = {
                'hoodie.table.name': f"{iDBSchema}_{iTable}",
                'hoodie.datasource.write.recordkey.field': 'uuid',
                'hoodie.datasource.write.partitionpath.field': 'date',
                'hoodie.datasource.write.precombine.field': 'ts',
                'hoodie.datasource.write.operation': 'bulk_insert',
                'hoodie.bulkinsert.shuffle.parallelism': '2',
                'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
                'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
                'hoodie.cleaner.commits.retained': '10',
                'hoodie.keep.min.commits': '20',
                'hoodie.keep.max.commits': '30'
            }

            # Write to Hudi table
            print(f"Writing to Hudi table at: {table_path}")
            # Check if we have data to write
            record_count = final_df.count()
            print(f"Number of records to write: {record_count}")

            if record_count > 0:
                final_df.write \
                    .format("org.apache.hudi") \
                    .options(**hudiOptions) \
                    .mode("overwrite" if batch_id == 0 else "append") \
                    .save(table_path)
            else:
                print("No records to write in this batch")

            print(f"Successfully wrote data to {table_path}")

            # Register table in Hive metastore
            if batch_id == 0:
                spark_session.sql(f"""
                    CREATE TABLE IF NOT EXISTS {iDBSchema}.{iTable}
                    USING hudi
                    LOCATION '{table_path}'
                """)
                print(f"Table {iDBSchema}.{iTable} registered")

            # Verify the write by reading back
            print("Verifying written data:")
            read_df = spark_session.read.format("hudi").load(table_path)
            read_df.show(5)

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

def main():
    try:
        # Initialize Spark session
        print("Creating Spark session...")
        spark = create_spark_session()

        print("Spark Session created successfully")

        # Show available databases
        print("Available databases:")
        spark.sql("SHOW DATABASES").show()

        # Define schema and table names
        iDBSchema = "kafka_hudi"
        iTable = "iot_events"

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
        processor = create_kafka_to_hudi_processor(spark, iDBSchema, iTable)

        # Write stream using foreachBatch
        query = df_kafka.writeStream \
            .foreachBatch(processor) \
            .outputMode("append") \
            .option("checkpointLocation", f"gs://osd-data/checkpoints/{iDBSchema}.db/{iTable}") \
            .trigger(once=True) \
            .start()

        # Wait for the streaming to finish
        query.awaitTermination()

        print("Stream processing completed")

    except Exception as e:
        print(f"Error in main process: {str(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()