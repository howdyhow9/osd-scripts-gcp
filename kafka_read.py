from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *
import os
from google.cloud import storage
import sys

# Set up GCS client and download the file
client = storage.Client()
bucket = client.get_bucket("osd-scripts")
blob = bucket.blob("spark_config_delta.py")
blob.download_to_filename("/tmp/spark_config_delta.py")

# Add the directory to system path
sys.path.insert(0, '/tmp')

# Import your file as a module
from spark_config_delta import create_spark_session

def kafka_to_delta(df, batch_id):
    print("Processing batch:", batch_id)
    iDBSchema = "kafka_delta"
    iTable = "kafka"
    table_loc = f"gs://osd-data/{iDBSchema}.db/{iTable}"  # Fixed table location path

    try:
        # Handle Kafka message format
        kafka_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

        # Write batch to Delta Lake
        kafka_df.write \
            .format("delta") \
            .mode("append") \
            .save(table_loc)

        # Create or update table metadata
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {iDBSchema}")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {iDBSchema}.{iTable}
            USING delta
            LOCATION '{table_loc}'
        """)

        # Display preview of the batch
        print("Preview of batch data:")
        kafka_df.show(5)

        print(f"Successfully wrote batch {batch_id} to {iDBSchema}.{iTable}")

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
        raise

# Initialize Spark session
spark = create_spark_session()
iDBSchema = "kafka_delta"
iTable = "kafka"

# Read from Kafka stream with security configurations
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "osds-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "osds-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Write stream using foreachBatch
query = df_kafka.writeStream \
    .foreachBatch(kafka_to_delta) \
    .outputMode("append") \
    .option("checkpointLocation", f"gs://osd-data/checkpoints/{iDBSchema}/{iTable}") \
    .trigger(once=True) \
    .start()

# Wait for the streaming to finish
query.awaitTermination()

# Verify the table after processing
try:
    result = spark.sql(f"SELECT COUNT(*) as count FROM {iDBSchema}.{iTable}")
    print("Final table count:")
    result.show()
except Exception as e:
    print(f"Error verifying table: {str(e)}")

# Clean up
spark.stop()