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
blob = bucket.blob("spark_config.py")
blob.download_to_filename("/tmp/spark_config.py")

# Add the directory to system path
sys.path.insert(0, '/tmp')

# Modify your spark_config.py to include these configurations in the create_spark_session function:
def create_spark_session_with_hudi():
    spark = SparkSession.builder \
        .appName("Hudi Ingestion") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .getOrCreate()
    return spark

def IngestHudiCSVHeader(spark, iDBSchema, iTable, iFilePath):
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

        # Define Hudi specific configurations
        hudiOptions = {
            'hoodie.table.name': iTable,
            'hoodie.datasource.write.recordkey.field': 'id',  # Replace with your primary key field
            'hoodie.datasource.write.partitionpath.field': '',  # Add partition field if needed
            'hoodie.datasource.write.table.name': iTable,
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.datasource.write.precombine.field': 'id',  # Field for de-duplication
            'hoodie.upsert.shuffle.parallelism': '2',
            'hoodie.insert.shuffle.parallelism': '2',
            'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
            'hoodie.cleaner.commits.retained': '10'
        }

        # Write to Hudi table
        menu_csv.write \
            .format("org.apache.hudi") \
            .options(**hudiOptions) \
            .mode("overwrite") \
            .option("checkpointLocation", f"gs://osd-data/checkpoints/{iDBSchema}/{iTable}") \
            .saveAsTable(f"{iDBSchema}.{iTable}")

        print(f"Successfully ingested {iTable} into {iDBSchema}")

        # Perform compaction
        spark.sql(f"""
            CALL {iDBSchema}.{iTable}_syncview_000.run_compaction(
                operation => 'schedule',
                schedule_inline => true
            )
        """)

    except Exception as e:
        print(f"Error processing {iFilePath}: {str(e)}")
        raise

def main():
    # Create Spark session with Hudi support
    spark = create_spark_session_with_hudi()

    try:
        # Show available databases
        print("Available databases:")
        spark.sql("show databases").show()

        # Define tables to ingest
        tables_to_ingest = [
            ("restaurant_hudi", "menu", "gs://osd-data/source/menu_items.csv"),
            ("restaurant_hudi", "orders", "gs://osd-data/source/order_details.csv"),
            ("restaurant_hudi", "db_dictionary", "gs://osd-data/source/data_dictionary.csv")
        ]

        # Ingest all tables
        for schema, table, path in tables_to_ingest:
            IngestHudiCSVHeader(spark, schema, table, path)

        print("All tables ingested successfully")

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()