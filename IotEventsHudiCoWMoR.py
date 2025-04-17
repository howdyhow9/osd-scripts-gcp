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

# Initialize Spark session
spark = create_spark_session()

# Define schema matching iot_events
schema = StructType([
    StructField("uuid", StringType(), False),
    StructField("ts", StringType(), True),
    StructField("consumption", DoubleType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("minute", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("key", StringType(), True),
    StructField("kafka_key", StringType(), True),
    StructField("kafka_timestamp", StringType(), True),
    StructField("processing_time", StringType(), True),
    StructField("batch_id", LongType(), True)
])

# Sample data
data = [
    ("IoT_01", "2025-04-17 04:00:00", 50.25, 4, 17, 4, 0, "2025/04/17", "IoT_01_2025-04-17_04:00:00", "IoT_01_2025-04-17_04:00:00", "2025-04-17 04:00:00", "2025-04-17 04:05:00", 2),
    ("IoT_02", "2025-04-17 04:00:00", 60.75, 4, 17, 4, 0, "2025/04/17", "IoT_02_2025-04-17_04:00:00", "IoT_02_2025-04-17_04:00:00", "2025-04-17 04:00:00", "2025-04-17 04:05:00", 2)
]
df = spark.createDataFrame(data, schema)

# Hudi configurations for iot_events (COW)
base_path_cow = "gs://osd-data/kafka_hudi.db/iot_events"
table_name_cow = "iot_events"
hudi_options_cow = {
    "hoodie.table.name": table_name_cow,
    "hoodie.datasource.write.recordkey.field": "uuid",
    "hoodie.datasource.write.partitionpath.field": "",  # No partitioning
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "kafka_hudi",
    "hoodie.datasource.hive_sync.table": table_name_cow,
    "hoodie.datasource.hive_sync.partition_fields": ""
}

# Function to write to Hudi
def write_hudi(df, base_path, hudi_options, table_type, operation="upsert  mode="append"):
df.write.format("hudi") \
    .options(**hudi_options, **{
    "hoodie.datasource.write.table.type": table_type,
    "hoodie.datasource.write.operation": operation
}) \
    .mode(mode) \
    .save(base_path)

# Function to read from Hudi
def read_hudi(base_path, query_type="snapshot", begin_time=None):
    reader = spark.read.format("hudi")
    if query_type == "incremental":
        reader = reader.option("hoodie.datasource.query.type", "incremental") \
            .option("hoodie.datasource.read.begin.instanttime", begin_time)
    return reader.load(base_path)

# --- Test COW (iot_events) ---
print("Testing COW on iot_events")

# Test 1: Insert (COW)
write_hudi(df, base_path_cow, hudi_options_cow, "COPY_ON_WRITE", operation="insert", mode="append")
print("COW Insert:")
read_hudi(base_path_cow).select("uuid", "ts", "consumption", "date").show()

# Test 2: Upsert (COW)
update_data = [
    ("IoT_01", "2025-04-17 04:00:00", 55.50, 4, 17, 4, 0, "2025/04/17", "IoT_01_2025-04-17_04:00:00_updated", "IoT_01_2025-04-17_04:00:00_updated", "2025-04-17 04:00:00", "2025-04-17 04:06:00", 2),
    ("IoT_03", "2025-04-17 04:00:00", 70.00, 4, 17, 4, 0, "2025/04/17", "IoT_03_2025-04-17_04:00:00", "IoT_03_2025-04-17_04:00:00", "2025-04-17 04:00:00", "2025-04-17 04:06:00", 2)
]
update_df = spark.createDataFrame(update_data, schema)
write_hudi(update_df, base_path_cow, hudi_options_cow, "COPY_ON_WRITE", operation="upsert")
print("COW Upsert:")
read_hudi(base_path_cow).select("uuid", "ts", "consumption", "date").show()

# Test 3: Delete (COW)
delete_data = [("IoT_02", "2025-04-17 04:00:00", 60.75, 4, 17, 4, 0, "2025/04/17", "IoT_02_2025-04-17_04:00:00", "IoT_02_2025-04-17_04:00:00", "2025-04-17 04:00:00", "2025-04-17 04:05:00", 2)]
delete_df = spark.createDataFrame(delete_data, schema)
write_hudi(delete_df, base_path_cow, hudi_options_cow, "COPY_ON_WRITE", operation="delete")
print("COW Delete:")
read_hudi(base_path_cow).select("uuid", "ts", "consumption", "date").show()

# Test 4: Incremental Query (COW)
commits = spark.read.format("hudi").load(base_path_cow).select("_hoodie_commit_time").distinct().collect()
begin_time = min([row._hoodie_commit_time for row in commits])
print("COW Incremental Query:")
read_hudi(base_path_cow, query_type="incremental", begin_time=begin_time).select("uuid", "ts", "consumption", "date").show()

# Stop Spark session
spark.stop()