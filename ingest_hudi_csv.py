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
blob = bucket.blob("spark_config_delta.py")
blob.download_to_filename("/tmp/spark_config_delta.py")

# Add the directory to system path
sys.path.insert(0, '/tmp')

def create_spark_session_with_hudi():
    spark = SparkSession.builder \
        .appName("Hudi Ingestion") \
        .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.14.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("hive.metastore.warehouse.dir", "gs://osd-data/") \
        .getOrCreate()

    return spark

def IngestHudiCSVHeader(spark, iDBSchema, iTable, iFilePath):
    try:
        # Read the CSV file
        print(f"Reading CSV from: {iFilePath}")
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(iFilePath)

        print(f"Preview of data:")
        df.show(5)
        print(f"Schema:")
        df.printSchema()

        # Get table location path
        table_path = f"gs://osd-data/{iDBSchema}.db/{iTable}"

        # Get primary key column (assuming first column if not 'id')
        pk_column = 'id' if 'id' in df.columns else df.columns[0]
        print(f"Using {pk_column} as the primary key")

        # Write to Hudi table
        print(f"Writing to Hudi table at: {table_path}")

        hudiOptions = {
            'hoodie.table.name': f"{iDBSchema}_{iTable}",
            'hoodie.datasource.write.recordkey.field': pk_column,
            'hoodie.datasource.write.precombine.field': pk_column,
            'hoodie.datasource.write.operation': 'bulk_insert',
            'hoodie.bulkinsert.shuffle.parallelism': '2'
        }

        # Write using DataFrameWriter
        df.write \
            .format("org.apache.hudi") \
            .options(**hudiOptions) \
            .mode("overwrite") \
            .save(table_path)

        print(f"Successfully written data to {table_path}")

        # Verify the write by reading back
        print("Verifying written data:")
        read_df = spark.read.format("org.apache.hudi").load(table_path)
        read_df.show(5)

    except Exception as e:
        print(f"Error processing {iFilePath}: {str(e)}")
        raise

def main():
    spark = create_spark_session_with_hudi()

    try:
        print("Spark Session created successfully")

        # Define tables to ingest
        tables_to_ingest = [
            ("restaurant_hudi", "menu", "gs://osd-data/source/menu_items.csv"),
            ("restaurant_hudi", "orders", "gs://osd-data/source/order_details.csv"),
            ("restaurant_hudi", "db_dictionary", "gs://osd-data/source/data_dictionary.csv")
        ]

        # Ingest all tables
        for schema, table, path in tables_to_ingest:
            print(f"\nProcessing table: {schema}.{table}")
            IngestHudiCSVHeader(spark, schema, table, path)

        print("\nAll tables ingested successfully")

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()