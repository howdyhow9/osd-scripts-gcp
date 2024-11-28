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
blob = bucket.blob("spark_config_iceberg.py")
blob.download_to_filename("/tmp/spark_config_iceberg.py")

# Add the directory to system path
sys.path.insert(0, '/tmp')

# Import your file as a module
from spark_config_iceberg import create_spark_session

def IngestIcebergCSVHeader(spark, iDBSchema, iTable, iFilePath):
    try:
        # Set Iceberg properties to avoid locking issues
        spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set("spark.sql.catalog.spark_catalog.type", "hive")
        spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")

        # Disable lock creation
        spark.conf.set("spark.sql.iceberg.create-table-lock-enabled", "false")
        spark.conf.set("spark.sql.iceberg.lock-impl", "org.apache.iceberg.common.DynamicLockConfig")

        # Read the CSV file with error handling
        print(f"Reading CSV from: {iFilePath}")
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .load(iFilePath)

        print(f"Preview of data:")
        df.show(5)
        print(f"Schema:")
        df.printSchema()

        # Create schema if not exists
        try:
            spark.sql(f"create database if not exists {iDBSchema}")
            print(f"Schema {iDBSchema} created or already exists")
        except Exception as e:
            print(f"Error creating schema {iDBSchema}: {str(e)}")
            raise

        # Get table location path
        table_path = f"gs://osd-data/{iDBSchema}.db/{iTable}"

        # Drop existing table if it exists
        spark.sql(f"DROP TABLE IF EXISTS {iDBSchema}.{iTable}")

        # Create Iceberg table with basic properties
        create_table_sql = f"""
        CREATE TABLE {iDBSchema}.{iTable} (
            {', '.join([f"{col} {str(dtype).replace('Type','')}"
                        for col, dtype in df.dtypes])}
        )
        USING iceberg
        LOCATION '{table_path}'
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'write.object-storage.enabled' = 'true',
            'write.data.path' = '{table_path}/data',
            'write.metadata.path' = '{table_path}/metadata',
            'format-version' = '2'
        )
        """

        spark.sql(create_table_sql)

        # Write data to Iceberg table using DataFrame write API
        print(f"Writing to Iceberg table at: {table_path}")
        df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .option("merge-schema", "true") \
            .option("check-ordering", "false") \
            .option("isolation-level", "snapshot") \
            .saveAsTable(f"{iDBSchema}.{iTable}")

        print(f"Successfully written data to {table_path}")

        # Verify the write by reading back
        print("Verifying written data:")
        read_df = spark.table(f"{iDBSchema}.{iTable}")
        read_df.show(5)

    except Exception as e:
        print(f"Error processing {iFilePath}: {str(e)}")
        raise

def main():
    spark = create_spark_session()

    try:
        print("Spark Session created successfully")

        # Show available databases
        print("Available databases:")
        spark.sql("show databases").show()

        # Define tables to ingest
        tables_to_ingest = [
            ("restaurant_iceberg", "menu", "gs://osd-data/source/menu_items.csv"),
            ("restaurant_iceberg", "orders", "gs://osd-data/source/order_details.csv"),
            ("restaurant_iceberg", "db_dictionary", "gs://osd-data/source/data_dictionary.csv")
        ]

        # Ingest all tables
        for schema, table, path in tables_to_ingest:
            print(f"\nProcessing table: {schema}.{table}")
            IngestIcebergCSVHeader(spark, schema, table, path)

        print("\nAll tables ingested successfully")

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()