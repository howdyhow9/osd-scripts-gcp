from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os

def create_spark_session():
    """Initialize Spark session with Hudi configurations"""
    spark = SparkSession.builder \
        .appName("HudiCoWMoRIotEventsDemo") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .getOrCreate()
    return spark

def create_sample_iot_data(spark):
    """Create sample data matching iot_events schema"""
    schema = StructType([
        StructField("uuid", StringType(), False),
        StructField("ts", StringType(), False),  # Temporarily StringType for input
        StructField("consumption", StringType(), False),
        StructField("month", StringType(), False),
        StructField("day", StringType(), False),
        StructField("hour", StringType(), False),
        StructField("minute", StringType(), False),
        StructField("date", StringType(), False),
        StructField("key", StringType(), False),
        StructField("kafka_key", StringType(), True),
        StructField("kafka_timestamp", StringType(), True),  # Temporarily StringType for input
        StructField("processing_time", StringType(), True),  # Temporarily StringType for input
        StructField("batch_id", StringType(), True)
    ])

    data = [
        ("device1", "2025-04-16 10:00:00", "100.5", "04", "16", "10", "00", "2025-04-16", "key1", "kafka_key1", "2025-04-16 10:01:00", "2025-04-16 10:02:00", "1"),
        ("device2", "2025-04-16 10:01:00", "200.3", "04", "16", "10", "01", "2025-04-16", "key2", "kafka_key2", "2025-04-16 10:02:00", "2025-04-16 10:03:00", "1"),
        ("device3", "2025-04-16 10:02:00", "150.7", "04", "16", "10", "02", "2025-04-16", "key3", "kafka_key3", "2025-04-16 10:03:00", "2025-04-16 10:04:00", "1")
    ]

    # Create DataFrame with string timestamps
    df = spark.createDataFrame(data, schema)

    # Convert string timestamps to TimestampType
    return df.withColumn("ts", to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("kafka_timestamp", to_timestamp(col("kafka_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("processing_time", to_timestamp(col("processing_time"), "yyyy-MM-dd HH:mm:ss"))

def update_iot_data(spark):
    """Create sample update data for iot_events"""
    schema = StructType([
        StructField("uuid", StringType(), False),
        StructField("ts", StringType(), False),  # Temporarily StringType for input
        StructField("consumption", StringType(), False),
        StructField("month", StringType(), False),
        StructField("day", StringType(), False),
        StructField("hour", StringType(), False),
        StructField("minute", StringType(), False),
        StructField("date", StringType(), False),
        StructField("key", StringType(), False),
        StructField("kafka_key", StringType(), True),
        StructField("kafka_timestamp", StringType(), True),  # Temporarily StringType for input
        StructField("processing_time", StringType(), True),  # Temporarily StringType for input
        StructField("batch_id", StringType(), True)
    ])

    updates = [
        ("device1", "2025-04-16 11:00:00", "110.8", "04", "16", "11", "00", "2025-04-16", "key1", "kafka_key1", "2025-04-16 11:01:00", "2025-04-16 11:02:00", "2"),
        ("device4", "2025-04-16 11:01:00", "300.1", "04", "16", "11", "01", "2025-04-16", "key4", "kafka_key4", "2025-04-16 11:02:00", "2025-04-16 11:03:00", "2")
    ]

    # Create DataFrame with string timestamps
    df = spark.createDataFrame(updates, schema)

    # Convert string timestamps to TimestampType
    return df.withColumn("ts", to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("kafka_timestamp", to_timestamp(col("kafka_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("processing_time", to_timestamp(col("processing_time"), "yyyy-MM-dd HH:mm:ss"))

def write_hudi_table(df, table_name, table_path, table_type, schema_name="kafka_hudi"):
    """Write DataFrame to Hudi table (CoW or MoR)"""
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'uuid',
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.table.type': table_type,
        'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
        'hoodie.cleaner.commits.retained': '10',
        'hoodie.keep.min.commits': '20',
        'hoodie.keep.max.commits': '30',
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.database': schema_name,
        'hoodie.datasource.hive_sync.table': table_name,
        'hoodie.datasource.hive_sync.use_jdbc': 'false'
    }

    if table_type == 'MERGE_ON_READ':
        hudi_options['hoodie.datasource.write.index.type'] = 'BLOOM'
        hudi_options['hoodie.compaction.strategy'] = 'org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy'
        hudi_options['hoodie.compaction.logfile.size.threshold'] = '134217728'  # 128MB

    df.write \
        .format("hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(table_path)

    print(f"Successfully wrote to {table_type} table: {table_name} at {table_path}")

def main():
    try:
        # Initialize Spark session
        spark = create_spark_session()

        # Create sample iot_events data
        df = create_sample_iot_data(spark)
        print("Initial iot_events data:")
        df.show(truncate=False)

        # Define schema and table paths
        schema_name = "kafka_hudi"
        cow_table_name = "iot_events_cow"
        cow_table_path = "gs://osd-data/kafka_hudi.db/iot_events_cow"
        mor_table_name = "iot_events_mor"
        mor_table_path = "gs://osd-data/kafka_hudi.db/iot_events_mor"

        # Create schema if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
        print(f"Schema {schema_name} created or already exists")

        # Write to CoW table
        write_hudi_table(df, cow_table_name, cow_table_path, "COPY_ON_WRITE", schema_name)

        # Write to MoR table
        write_hudi_table(df, mor_table_name, mor_table_path, "MERGE_ON_READ", schema_name)

        # Perform updates
        update_df = update_iot_data(spark)
        print("Update data for iot_events:")
        update_df.show(truncate=False)

        # Apply updates to both tables
        write_hudi_table(update_df, cow_table_name, cow_table_path, "COPY_ON_WRITE", schema_name)
        write_hudi_table(update_df, mor_table_name, mor_table_path, "MERGE_ON_READ", schema_name)

        # Read and verify CoW table
        print("Reading CoW table (iot_events_cow):")
        cow_df = spark.read.format("hudi").load(cow_table_path)
        cow_df.select("uuid", "ts", "consumption", "date", "processing_time").show(truncate=False)

        # Read and verify MoR table (read-optimized view)
        print("Reading MoR table (iot_events_mor, read-optimized):")
        mor_df = spark.read.format("hudi").load(mor_table_path)
        mor_df.select("uuid", "ts", "consumption", "date", "processing_time").show(truncate=False)

        # Read MoR table (real-time view)
        print("Reading MoR table (iot_events_mor, real-time):")
        mor_rt_df = spark.read.option("as.of.instant", "LATEST").format("hudi").load(mor_table_path)
        mor_rt_df.select("uuid", "ts", "consumption", "date", "processing_time").show(truncate=False)

        # Show table statistics
        for table_path, table_type in [(cow_table_path, "CoW"), (mor_table_path, "MoR")]:
            df = spark.read.format("hudi").load(table_path)
            print(f"{table_type} Table Statistics for iot_events:")
            df.select(
                lit(table_type).alias("table_type"),
                count("*").alias("total_records"),
                countDistinct("uuid").alias("unique_devices"),
                max("ts").alias("latest_timestamp"),
                min("consumption").alias("min_consumption"),
                max("consumption").alias("max_consumption")
            ).show(truncate=False)

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()