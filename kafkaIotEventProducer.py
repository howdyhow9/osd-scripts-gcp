from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *
import os
from google.cloud import storage
import sys
from datetime import datetime

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
    print("Starting data generation...")

    # Create sample data first
    from datetime import datetime
    import random

    data = []
    current_time = datetime.now()

    for i in range(num_records):
        device_id = f"IoT_{random.randint(1,4):02d}"
        consumption = round(random.uniform(40.0, 50.0), 2)
        data.append({
            "uuid": device_id,
            "ts": current_time,
            "consumption": consumption,
            "month": str(current_time.month),
            "day": str(current_time.day),
            "hour": str(current_time.hour),
            "minute": str(current_time.minute),
            "date": current_time.strftime("%Y/%m/%d"),
            "key": f"{device_id}_{current_time.strftime('%Y-%m-%d %H:%M:%S')}"
        })

    # Create DataFrame directly with all fields
    df = spark.createDataFrame(data)

    print("Generated DataFrame schema:")
    df.printSchema()

    print("Sample of generated data:")
    df.show(5, truncate=False)

    return df

def write_to_kafka(df, bootstrap_servers, topic):
    """Write DataFrame to Kafka topic"""

    print("Preparing data for Kafka...")
    print("DataFrame schema before Kafka preparation:")
    df.printSchema()

    # Convert DataFrame to JSON string
    kafka_df = df.select(
        F.col("key").cast("string").alias("key"),
        F.to_json(F.struct("*")).alias("value")
    )

    print("Kafka DataFrame schema:")
    kafka_df.printSchema()
    print("Sample of Kafka data:")
    kafka_df.show(5, truncate=False)

    # Write to Kafka
    print(f"Writing to Kafka topic: {topic}")
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("topic", topic) \
        .save()

def main():
    # Configuration
    bootstrap_servers = "osds-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
    topic = "osds-topic"
    num_records = 5000

    try:
        # Create Spark session using imported method
        print("Creating Spark session...")
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
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")
    finally:
        if 'spark' in locals():
            spark.stop()
            print("Spark session stopped")

if __name__ == "__main__":
    main()