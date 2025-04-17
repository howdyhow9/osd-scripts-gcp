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

# Define schema matching iot_events - using StringType for timestamp fields to match existing schema
schema = StructType([
    StructField("uuid", StringType(), False),
    StructField("ts", StringType(), True),  # Keep as StringType, not TimestampType
    StructField("consumption", DoubleType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("minute", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("key", StringType(), True),
    StructField("kafka_key", StringType(), True),
    StructField("kafka_timestamp", StringType(), True),  # Keep as StringType
    StructField("processing_time", StringType(), True),  # Keep as StringType
    StructField("batch_id", LongType(), True)
])

# Sample data - no need to convert timestamp strings
data = [
    ("IoT_01", "2025-04-17 04:00:00", 50.25, 4, 17, 4, 0, "2025/04/17", "IoT_01_2025-04-17_04:00:00", "IoT_01_2025-04-17_04:00:00", "2025-04-17 04:00:00", "2025-04-17 04:05:00", 2),
    ("IoT_02", "2025-04-17 04:00:00", 60.75, 4, 17, 4, 0, "2025/04/17", "IoT_02_2025-04-17_04:00:00", "IoT_02_2025-04-17_04:00:00", "2025-04-17 04:00:00", "2025-04-17 04:05:00", 2)
]

# Create dataframe - keep timestamp fields as strings
df = spark.createDataFrame(data, schema)

# Hudi configurations for iot_events (COW)
base_path_cow = "gs://osd-data/kafka_hudi.db/iot_events"
table_name_cow = "kafka_hudi_iot_events"
hudi_options_cow = {
    "hoodie.table.name": "kafka_hudi_iot_events",
    "hoodie.datasource.write.recordkey.field": "uuid",
    "hoodie.datasource.write.partitionpath.field": "",  # No partitioning
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "kafka_hudi",
    "hoodie.datasource.hive_sync.table": "kafka_hudi_iot_events",
    "hoodie.datasource.hive_sync.partition_fields": "",
    "hoodie.datasource.hive_sync.jdbcurl": "jdbc:postgresql://postgres:5432/hive_metastore",
    "hoodie.datasource.hive_sync.username": "hive",
    "hoodie.datasource.hive_sync.password": "GUYgsjsj@123",
    "hoodie.datasource.hive_sync.mode": "hms",
    # Schema evolution options
    "hoodie.datasource.write.reconcile.schema": "true",
    "hoodie.schema.on.read.enable": "true",
    "hoodie.avro.schema.validate": "false"  # Turn off strict validation
}

# Hudi configurations for new MOR table
base_path_mor = "gs://osd-data/kafka_hudi.db/iot_events_mor"
table_name_mor = "kafka_hudi_iot_events_mor"
hudi_options_mor = {
    "hoodie.table.name": "kafka_hudi_iot_events_mor",
    "hoodie.datasource.write.recordkey.field": "uuid",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "kafka_hudi",
    "hoodie.datasource.hive_sync.table": "kafka_hudi_iot_events_mor",
    "hoodie.datasource.hive_sync.partition_fields": "",
    "hoodie.datasource.hive_sync.jdbcurl": "jdbc:postgresql://postgres:5432/hive_metastore",
    "hoodie.datasource.hive_sync.username": "hive",
    "hoodie.datasource.hive_sync.password": "GUYgsjsj@123",
    "hoodie.datasource.hive_sync.mode": "hms",
    # Schema evolution options
    "hoodie.datasource.write.reconcile.schema": "true",
    "hoodie.schema.on.read.enable": "true",
    "hoodie.avro.schema.validate": "false"  # Turn off strict validation
}

# Function to write to Hudi
def write_hudi(df, base_path, hudi_options, table_type, operation="upsert", mode="append"):
    options = hudi_options.copy()
    options["hoodie.datasource.write.table.type"] = table_type
    options["hoodie.datasource.write.operation"] = operation

    try:
        df.write.format("hudi") \
            .options(**options) \
            .mode(mode) \
            .save(base_path)
        print(f"Successfully wrote data with operation '{operation}' to {base_path}")
    except Exception as e:
        print(f"Error writing data: {str(e)}")
        raise

# Function to read from Hudi
def read_hudi(base_path, query_type="snapshot", begin_time=None):
    try:
        reader = spark.read.format("hudi")
        if query_type == "incremental":
            reader = reader.option("hoodie.datasource.query.type", "incremental") \
                .option("hoodie.datasource.read.begin.instanttime", begin_time)
        elif query_type == "read_optimized":
            reader = reader.option("hoodie.datasource.query.type", "read_optimized")

        df = reader.load(base_path)
        return df
    except Exception as e:
        print(f"Read error: {str(e)}")
        return spark.createDataFrame([], schema)

# Function to safely display dataframes
def safe_show(df, cols=None, n=20):
    try:
        if cols:
            df.select(*cols).show(n)
        else:
            df.show(n)
    except Exception as e:
        print(f"Display error: {str(e)}")
        print("Trying alternative display method...")
        try:
            df.createOrReplaceTempView("temp_view")
            if cols:
                cols_str = ", ".join(cols)
                spark.sql(f"SELECT {cols_str} FROM temp_view").show(n)
            else:
                spark.sql("SELECT * FROM temp_view").show(n)
        except Exception as e2:
            print(f"Alternative display also failed: {str(e2)}")

# Update data - keeping timestamps as strings
update_data = [
    ("IoT_01", "2025-04-17 04:00:00", 55.50, 4, 17, 4, 0, "2025/04/17", "IoT_01_2025-04-17_04:00:00_updated", "IoT_01_2025-04-17_04:00:00_updated", "2025-04-17 04:00:00", "2025-04-17 04:06:00", 2),
    ("IoT_03", "2025-04-17 04:00:00", 70.00, 4, 17, 4, 0, "2025/04/17", "IoT_03_2025-04-17_04:00:00", "IoT_03_2025-04-17_04:00:00", "2025-04-17 04:00:00", "2025-04-17 04:06:00", 2)
]
update_df = spark.createDataFrame(update_data, schema)

# Delete data - keeping timestamps as strings
delete_data = [
    ("IoT_02", "2025-04-17 04:00:00", 60.75, 4, 17, 4, 0, "2025/04/17", "IoT_02_2025-04-17_04:00:00", "IoT_02_2025-04-17_04:00:00", "2025-04-17 04:00:00", "2025-04-17 04:05:00", 2)
]
delete_df = spark.createDataFrame(delete_data, schema)

try:
    # First, let's check if the existing table has data
    print("Checking existing table schema...")
    try:
        existing_df = spark.read.format("hudi").load(base_path_cow)
        print("Existing table schema:")
        existing_df.printSchema()
    except Exception as e:
        print(f"Could not read existing table: {str(e)}")

    # --- Test COW (iot_events) ---
    print("Testing COW on iot_events")

    # Test 1: Insert (COW)
    write_hudi(df, base_path_cow, hudi_options_cow, "COPY_ON_WRITE", operation="insert", mode="append")
    print("COW Insert:")
    result_df = read_hudi(base_path_cow)
    safe_show(result_df, ["uuid", "ts", "consumption", "date"])

    # Test 2: Upsert (COW)
    write_hudi(update_df, base_path_cow, hudi_options_cow, "COPY_ON_WRITE")
    print("COW Upsert:")
    result_df = read_hudi(base_path_cow)
    safe_show(result_df, ["uuid", "ts", "consumption", "date"])

    # Test 3: Delete (COW)
    write_hudi(delete_df, base_path_cow, hudi_options_cow, "COPY_ON_WRITE", operation="delete")
    print("COW Delete:")
    result_df = read_hudi(base_path_cow)
    safe_show(result_df, ["uuid", "ts", "consumption", "date"])

    # Test 4: Incremental Query (COW) with error handling
    try:
        commits = spark.read.format("hudi").load(base_path_cow).select("_hoodie_commit_time").distinct().collect()
        if commits:
            begin_time = min([row._hoodie_commit_time for row in commits])
            print("COW Incremental Query:")
            result_df = read_hudi(base_path_cow, query_type="incremental", begin_time=begin_time)
            safe_show(result_df, ["uuid", "ts", "consumption", "date"])
        else:
            print("No commits found for incremental query on COW table")
    except Exception as e:
        print(f"Error with incremental query on COW: {str(e)}")

    # --- Test MOR (iot_events_mor) ---
    print("Testing MOR on iot_events_mor")

    # For MOR table, we'll use overwrite mode for the first write
    write_hudi(df, base_path_mor, hudi_options_mor, "MERGE_ON_READ", operation="insert", mode="overwrite")
    print("MOR Insert:")
    result_df = read_hudi(base_path_mor)
    safe_show(result_df, ["uuid", "ts", "consumption", "date"])

    write_hudi(update_df, base_path_mor, hudi_options_mor, "MERGE_ON_READ")
    print("MOR Upsert:")
    result_df = read_hudi(base_path_mor)
    safe_show(result_df, ["uuid", "ts", "consumption", "date"])

    write_hudi(delete_df, base_path_mor, hudi_options_mor, "MERGE_ON_READ", operation="delete")
    print("MOR Delete:")
    result_df = read_hudi(base_path_mor)
    safe_show(result_df, ["uuid", "ts", "consumption", "date"])

    # Test 8: Read-Optimized Query (MOR)
    print("MOR Read-Optimized Query:")
    result_df = read_hudi(base_path_mor, query_type="read_optimized")
    safe_show(result_df, ["uuid", "ts", "consumption", "date"])

    # Test 9: Incremental Query (MOR) with error handling
    try:
        commits = spark.read.format("hudi").load(base_path_mor).select("_hoodie_commit_time").distinct().collect()
        if commits:
            begin_time = min([row._hoodie_commit_time for row in commits])
            print("MOR Incremental Query:")
            result_df = read_hudi(base_path_mor, query_type="incremental", begin_time=begin_time)
            safe_show(result_df, ["uuid", "ts", "consumption", "date"])
        else:
            print("No commits found for incremental query on MOR table")
    except Exception as e:
        print(f"Error with incremental query on MOR: {str(e)}")

except Exception as e:
    print(f"Error encountered: {str(e)}")
    import traceback
    traceback.print_exc()

finally:
    # Stop Spark session
    spark.stop()