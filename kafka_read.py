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

    # Write batch to Delta Lake
    df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(f"{iDBSchema}.{iTable}")

    # Display preview of the batch
    print("Preview of batch data:")
    df.show(5)

# Initialize Spark session
spark = create_spark_session()
iDBSchema = "restaurant_delta"
iTable = "kafka"

# Create schema if not exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {iDBSchema}")

# Read from Kafka stream with security configurations
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "osds-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "osds-topic") \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "PLAINTEXT") \
    .option("kafka.ssl.endpoint.identification.algorithm", "") \
    .option("fetchOffset.numRetries", "3") \
    .option("kafka.request.timeout.ms", "40000") \
    .option("kafka.session.timeout.ms", "30000") \
    .load()

# Write stream using foreachBatch
df_kafka.writeStream \
    .foreachBatch(kafka_to_delta) \
    .outputMode("append") \
    .option("checkpointLocation", f"gs://osd-data/checkpoints/{iDBSchema}/{iTable}") \
    .trigger(once=True) \
    .start()

# Clean up
spark.stop()