from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
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

# Import the downloaded configuration module
from spark_config import create_spark_session


def IngestJSONWithSampleSchema(spark, iDBSchema, iTable, iFilePath):
    try:
        # Step 1: Infer the schema from the first 100 rows
        sample_df = spark.read.format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat").load(iFilePath).limit(100)
        schema = sample_df.schema

        # Step 2: Use the inferred schema to load the full JSON file
        json_df = spark.read.format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat").schema(schema).load(iFilePath)

        print(f"Preview of data from {iFilePath}:")
        json_df.show(5)
        print(f"Schema of {iTable}:")
        json_df.printSchema()

        # Create schema if not exists with error handling
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {iDBSchema}")
        except Exception as e:
            print(f"Error creating schema {iDBSchema}: {str(e)}")
            raise

        # Write to Delta table with optimizations
        json_df.write \
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
        spark.sql("SHOW DATABASES").show()

        # Define JSON files to ingest
        tables_to_ingest = [
            ("osd_raw_db", "yelp_business", "gs://osd-data/source/yelp_academic_dataset_business.json"),
            ("osd_raw_db", "yelp_checkin", "gs://osd-data/source/yelp_academic_dataset_checkin.json"),
            ("osd_raw_db", "yelp_review", "gs://osd-data/source/yelp_academic_dataset_review.json"),
            ("osd_raw_db", "yelp_tip", "gs://osd-data/source/yelp_academic_dataset_tip.json"),
            ("osd_raw_db", "yelp_user", "gs://osd-data/source/yelp_academic_dataset_user.json")
            # Add other JSON files as needed
        ]

        # Ingest all tables
        for schema, table, path in tables_to_ingest:
            IngestJSONWithSampleSchema(spark, schema, table, path)

        print("All tables ingested successfully")

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
