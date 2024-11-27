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

# Import your file as a module
from spark_config_hudi import create_spark_session

def IngestHudiCSVHeader(spark, iDBSchema, iTable, iFilePath):
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

        # Hudi write options
        hudiOptions = {
            'hoodie.table.name': f"{iDBSchema}_{iTable}",
            'hoodie.datasource.write.recordkey.field': pk_column,
            'hoodie.datasource.write.precombine.field': pk_column,
            'hoodie.datasource.write.operation': 'bulk_insert',
            'hoodie.bulkinsert.shuffle.parallelism': '2',
            'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
            'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
            'hoodie.cleaner.commits.retained': '10',
            'hoodie.keep.min.commits': '20',
            'hoodie.keep.max.commits': '30'
        }

        # Write to Hudi table
        print(f"Writing to Hudi table at: {table_path}")
        df.write \
            .format("org.apache.hudi") \
            .options(**hudiOptions) \
            .mode("overwrite") \
            .save(table_path)

        print(f"Successfully written data to {table_path}")

        # Register table in Hive metastore
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {iDBSchema}.{iTable}
            USING hudi
            LOCATION '{table_path}'
        """)

        # Verify the write by reading back
        print("Verifying written data:")
        read_df = spark.read.format("hudi").load(table_path)
        read_df.show(5)

        # Run compaction
        print(f"Running compaction for {table_path}")
        spark.sql(f"CALL run_compaction('{table_path}')")

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