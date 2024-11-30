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

spark = create_spark_session()
iDBSchema = "restaurant_delta"
iTable = "kafka"

# Create schema if not exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {iDBSchema}")

# Read from Kafka stream
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "osds-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "osds-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Write to Delta Lake using writeStream
query = df_kafka \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"gs://osd-data/checkpoints/{iDBSchema}/{iTable}") \
    .table(f"{iDBSchema}.{iTable}")

# Wait for the processing to complete
query.processAllAvailable()  # This will process all available data and then stop
query.stop()  # Stop the query after processing is complete

# Clean up
spark.stop()