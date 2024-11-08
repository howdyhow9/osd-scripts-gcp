from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *
import os
import sys

# Set up GCS access for Spark
spark = SparkSession.builder \
    .appName("DeltaLakeGCSExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.gs.project.id", "osd-k8s") \
    .config("spark.hadoop.fs.gs.system.bucket", "osd-data") \
    .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/mnt/secrets/key.json") \
    .config("spark.sql.warehouse.dir", "gs://osd-data/") \
    .getOrCreate()

spark.sql("show databases").show()

# Function to ingest CSV data into Delta Lake
def IngestDeltaCSVHeader(iDBSchema, iTable, iFilePath):
    # Read the CSV file from GCS
    menu_csv = spark.read.format("csv").option("header", "true").load(iFilePath)
    menu_csv.show()

    # Create schema if not exists
    spark.sql(f"create schema if not exists {iDBSchema}")

    # Write the data to Delta table
    menu_csv.write.format("delta").mode("overwrite").saveAsTable(f"{iDBSchema}.{iTable}")

# Ingest CSV data into Delta tables from GCS
IngestDeltaCSVHeader("restaurant", "menu", "gs://osd-data/source-data/menu_items.csv")
IngestDeltaCSVHeader("restaurant", "orders", "gs://osd-data/source-data/order_details.csv")
IngestDeltaCSVHeader("restaurant", "db_dictionary", "gs://osd-data/source-data/restaurant_db_data_dictionary.csv")

# Example of querying and saving a new table (join operation)
# new_table_df = spark.sql("""
#     SELECT *
#     FROM restaurant.orders o
#     JOIN restaurant.menu m
#     ON o.item_id = m.menu_item_id
# """)
# new_table_df.write.format("delta").mode("overwrite").saveAsTable("restaurant.newtable")
