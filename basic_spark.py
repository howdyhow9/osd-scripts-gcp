from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *
import os
import sys

def create_spark_session():
    return SparkSession.builder \
        .appName("DeltaLakeGCSExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "gs://osd-data/") \
        .config("hive.metastore.warehouse.dir", "gs://osd-data/") \
        .config("javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres:5432/hive_metastore") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver") \
        .config("javax.jdo.option.ConnectionUserName", "hive") \
        .config("javax.jdo.option.ConnectionPassword", "GUYgsjsj@123") \
        .config("datanucleus.schema.autoCreateTables", "true") \
        .config("hive.metastore.schema.verification", "false") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/mnt/secrets/key.json") \
        .config("spark.hadoop.fs.gs.project.id", "your-gcp-project-id") \
        .config("spark.hadoop.fs.gs.system.bucket", "your-bucket-name") \
        .master("local[2]") \
        .enableHiveSupport()

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
            ("restaurant", "menu", "gs://osd-data/source/menu_items.csv"),
            ("restaurant", "orders", "gs://osd-data/source/order_details.csv"),
            ("restaurant", "db_dictionary", "gs://osd-data/source/restaurant_db_data_dictionary.csv")
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