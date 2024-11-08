from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *
import boto3
import os
import sys

s3 = boto3.client('s3')

# Specify S3 bucket and file
bucket_name = 'osd-scripts'
s3_file_key = 'spark_config.py'

# Local path where you want to save the file
local_file_path = '/tmp/spark_config.py'

# Download the file from S3
s3.download_file(bucket_name, s3_file_key, local_file_path)

# Add the directory containing the downloaded file to sys.path
sys.path.append('/tmp')

# Import the module
from spark_config import create_spark_session

spark = create_spark_session()
spark.sql("show databases").show()

def IngestDeltaCSVHeader(iDBSchema, iTable, iFilePath):
    menu_csv = spark.read.option("header", True).csv(iFilePath)
    menu_csv.show()
    spark.sql("create schema if not exists "+iDBSchema)
    menu_csv.write.format("delta").mode("overwrite").saveAsTable(iDBSchema+"."+iTable)

IngestDeltaCSVHeader("restaurant","menu", "s3a://osd-data/source-data/menu_items.csv")
IngestDeltaCSVHeader("restaurant","orders", "s3a://osd-data/source-data/order_details.csv")
IngestDeltaCSVHeader("restaurant","db_dictionary", "s3a://osd-data/source-data/restaurant_db_data_dictionary.csv")

#new_table_df = spark.sql("SELECT * FROM restaurant.orders o join restaurant.menu m on o.item_id = m.menu_item_id")
#new_table_df.write.format("delta").mode("overwrite").saveAsTable("restaurant.newtable")