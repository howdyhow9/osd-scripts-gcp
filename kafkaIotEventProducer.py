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

# Import spark session creation module
from spark_config_delta import create_spark_session

def generate_iot_data(spark, num_records=5000):
    """Generate IoT device data as a Spark DataFrame"""

    # Create schema for our data
    schema = StructType([
        StructField("uuid", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("consumption", DoubleType(), False)
    ])

    # Generate sample data
    from datetime import datetime
    import random

    data = []
    for _ in range(num_records):
        device_id = f"IoT_{random.randint(1,4):02d}"
        timestamp = datetime.now()
        consumption = round(random.uniform(40.0, 50.0), 2)
        data.append((device_id, timestamp, consumption))

    # Create DataFrame
    df = spark.createDataFrame(data, schema)

    # Add additional columns using F.* for clarity
    return df.withColumn("month", F.month("ts")) \
        .withColumn("day", F.dayofmonth("ts")) \
        .withColumn("hour", F.hour("ts")) \
        .withColumn("minute", F.minute("ts")) \
        .withColumn("date", F.date_format("ts", "yyyy/MM/dd")) \
        .withColumn("key", F.concat(F.col("uuid"), F.lit("_"),
                                    F.date_format("ts", "yyyy-MM-dd HH:mm:ss")))

def write_to_kafka(df, bootstrap_servers, topic):
    """Write DataFrame to Kafka topic"""

    # Convert DataFrame to JSON string
    kafka_df = df.select(
        F.col("key").cast("string").alias("key"),
        F.to_json(F.struct("*")).alias("value")
    )

    # Write to Kafka
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("topic", topic) \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .save()

def main():
    # Configuration
    bootstrap_servers = "osds-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
    topic = "osds-topic"
    num_records = 5000

    try:
        # Create Spark session using imported method
        spark = create_spark_session()

        # Generate data
        print(f"Generating {num_records} IoT events...")
        df = generate_iot_data(spark, num_records)

        # Write to Kafka
        print(f"Writing events to Kafka topic '{topic}'...")
        write_to_kafka(df, bootstrap_servers, topic)
        print("Successfully wrote events to Kafka")

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if 'spark' in locals():
            spark.stop()
            print("Spark session stopped")

if __name__ == "__main__":
    main()