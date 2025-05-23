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




def IngestDeltaCSVHeader(spark, iDBSchema, iTable, iFilePath):
    try:
        # Read the CSV file from GCS with error handling
        menu_csv = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .load(iFilePath)

        print(f"Preview of data from {iFilePath}:")
        menu_csv.show(5)
        print(f"Schema of {iTable}:")
        menu_csv.printSchema()

        # Create schema if not exists with error handling
        try:
            spark.sql(f"create schema if not exists {iDBSchema}")
        except Exception as e:
            print(f"Error creating schema {iDBSchema}: {str(e)}")
            raise

        # Write to Delta table with optimizations
        menu_csv.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("checkpointLocation", f"gs://osd-data/checkpoints/{iDBSchema}/{iTable}") \
            .saveAsTable(f"{iDBSchema}.{iTable}")

        print(f"Successfully ingested {iTable} into {iDBSchema}")

        # Optimize the table after write
        spark.sql(f"OPTIMIZE {iDBSchema}.{iTable}")

    except Exception as e:
        print(f"Error processing {iFilePath}: {str(e)}")
        raise

def main():
    spark = create_spark_session()

    try:
        # Show available databases
        print("Available databases:")
        spark.sql("show databases").show()

        # Define tables to ingest
        tables_to_ingest = [
            ("restaurant_delta", "menu", "gs://osd-data/source/menu_items.csv"),
            ("restaurant_delta", "orders", "gs://osd-data/source/order_details.csv"),
            ("restaurant_delta", "db_dictionary", "gs://osd-data/source/data_dictionary.csv")
        ]

        # Ingest all tables
        for schema, table, path in tables_to_ingest:
            IngestDeltaCSVHeader(spark, schema, table, path)

        print("All tables ingested successfully")

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()