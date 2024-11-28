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
blob = bucket.blob("spark_config_iceberg.py")  # Updated filename
blob.download_to_filename("/tmp/spark_config_iceberg.py")

# Add the directory to system path
sys.path.insert(0, '/tmp')

# Import your file as a module
from spark_config_iceberg import create_spark_session

def IngestIcebergCSVHeader(spark, iDBSchema, iTable, iFilePath):
    try:
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

        # Get primary key column (assuming first column if not 'id')
        pk_column = 'id' if 'id' in df.columns else df.columns[0]
        print(f"Using {pk_column} as the primary key")

        # Create Iceberg table with properties
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {iDBSchema}.{iTable} (
            {', '.join([f"{col} {str(dtype).replace('Type','')}"
                        for col, dtype in df.dtypes])}
        )
        USING iceberg
        PARTITIONED BY (years(ts))
        LOCATION '{table_path}'
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'write.metadata.compression-codec' = 'gzip',
            'write.metadata.metrics.default' = 'full',
            'write.distribution-mode' = 'hash',
            'write.object-storage.enabled' = 'true',
            'write.data.path' = '{table_path}/data',
            'write.metadata.path' = '{table_path}/metadata',
            'format-version' = '2'
        )
        """

        spark.sql(create_table_sql)

        # Write data to Iceberg table
        print(f"Writing to Iceberg table at: {table_path}")
        df.writeTo(f"{iDBSchema}.{iTable}") \
            .overwrite() \
            .execute()

        print(f"Successfully written data to {table_path}")

        # Verify the write by reading back
        print("Verifying written data:")
        read_df = spark.table(f"{iDBSchema}.{iTable}")
        read_df.show(5)

        # Run maintenance operations
        print(f"Running maintenance operations for {iDBSchema}.{iTable}")
        # Expire old snapshots
        spark.sql(f"CALL {iDBSchema}.system.expire_snapshots('{table_path}')")
        # Remove old metadata files
        spark.sql(f"CALL {iDBSchema}.system.remove_orphan_files('{table_path}')")

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