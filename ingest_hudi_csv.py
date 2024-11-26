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

def create_spark_session_with_hudi():
    spark = SparkSession.builder \
        .appName("Hudi Ingestion") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.sql.warehouse.dir", "file:/opt/spark/work-dir/spark-warehouse") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("hive.metastore.uris", "") \
        .config("spark.sql.legacy.createHiveTableByDefault", "false") \
        .getOrCreate()

    return spark

def IngestHudiCSVHeader(spark, iDBSchema, iTable, iFilePath):
    try:
        # Read the CSV file from GCS with error handling
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .load(iFilePath)

        print(f"Preview of data from {iFilePath}:")
        df.show(5)
        print(f"Schema of {iTable}:")
        df.printSchema()

        # Get table location path
        table_path = f"gs://osd-data/{iDBSchema}.db/{iTable}"

        # Get primary key column (assuming first column if not 'id')
        pk_column = 'id' if 'id' in df.columns else df.columns[0]
        print(f"Using {pk_column} as the primary key for table {iTable}")

        # Define Hudi specific configurations
        hudiOptions = {
            'hoodie.table.name': f"{iDBSchema}_{iTable}",
            'hoodie.datasource.write.recordkey.field': pk_column,
            'hoodie.datasource.write.precombine.field': pk_column,
            'hoodie.datasource.write.operation': 'bulk_insert',
            'hoodie.bulkinsert.shuffle.parallelism': '2',
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.hive_sync.database': iDBSchema,
            'hoodie.datasource.hive_sync.table': iTable,
            'hoodie.datasource.hive_sync.use_jdbc': 'false',
            'hoodie.datasource.hive_sync.mode': 'hms'
        }

        try:
            # Create database if not exists
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {iDBSchema}")
            print(f"Created or verified database {iDBSchema}")
        except Exception as e:
            print(f"Warning during database creation: {str(e)}")

        try:
            # Write to Hudi table
            df.write \
                .format("org.apache.hudi") \
                .options(**hudiOptions) \
                .mode("overwrite") \
                .save(table_path)

            print(f"Successfully written data to {table_path}")

            # Register table in Spark SQL
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {iDBSchema}.{iTable}
            USING hudi
            LOCATION '{table_path}'
            """
            spark.sql(create_table_sql)
            print(f"Successfully registered table {iDBSchema}.{iTable}")

            # Verify table creation
            spark.sql(f"SHOW TABLES IN {iDBSchema}").show()

            # Read back some data to verify
            verification_df = spark.read.format("hudi").load(table_path)
            print("Verification of written data:")
            verification_df.show(5)

        except Exception as e:
            print(f"Error during table write or registration: {str(e)}")
            raise

    except Exception as e:
        print(f"Error processing {iFilePath}: {str(e)}")
        raise

def main():
    spark = create_spark_session_with_hudi()

    try:
        print("Spark Session created successfully")
        print("Available databases before ingestion:")
        spark.sql("SHOW DATABASES").show()

        # Define tables to ingest
        tables_to_ingest = [
            ("restaurant", "menu", "gs://osd-data/source/menu_items.csv"),
            ("restaurant", "orders", "gs://osd-data/source/order_details.csv"),
            ("restaurant", "db_dictionary", "gs://osd-data/source/data_dictionary.csv")
        ]

        # Ingest all tables
        for schema, table, path in tables_to_ingest:
            print(f"\nProcessing table: {schema}.{table}")
            IngestHudiCSVHeader(spark, schema, table, path)

        print("\nAvailable databases after ingestion:")
        spark.sql("SHOW DATABASES").show()

        print("All tables ingested successfully")

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()