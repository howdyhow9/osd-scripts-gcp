from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *
import os
from google.cloud import storage
import sys
from datetime import datetime
import builtins
import time

# Set up GCS client and download the file
client = storage.Client()
bucket = client.get_bucket("osd-scripts")
blob = bucket.blob("spark_config_delta.py")
blob.download_to_filename("/tmp/spark_config_delta.py")

# Add the directory to system path
sys.path.insert(0, '/tmp')

# Import spark session creation module
from spark_config_delta import create_spark_session

def generate_iot_batch(spark, events_per_batch=60):
    """Generate a batch of IoT device data as a Spark DataFrame"""
    import random

    data = []
    current_time = datetime.now()

    for _ in range(events_per_batch):
        device_id = f"IoT_{random.randint(1,4):02d}"
        consumption = builtins.round(random.uniform(40.0, 50.0), 2)
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

    # Define schema explicitly
    schema = StructType([
        StructField("uuid", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("consumption", DoubleType(), False),
        StructField("month", StringType(), False),
        StructField("day", StringType(), False),
        StructField("hour", StringType(), False),
        StructField("minute", StringType(), False),
        StructField("date", StringType(), False),
        StructField("key", StringType(), False)
    ])

    return spark.createDataFrame(data, schema)

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
        .save()

    return kafka_df.count()

def continuous_generation(spark, bootstrap_servers, topic, events_per_minute=60):
    """Continuously generate and send events to Kafka"""
    batch_interval = 60  # seconds (1 minute)

    print(f"Starting continuous generation of {events_per_minute} events per minute...")
    print(f"Writing to Kafka topic: {topic}")

    while True:  # Run indefinitely
        try:
            batch_start_time = time.time()

            # Generate and write batch
            try:
                df = generate_iot_batch(spark, events_per_minute)
                events_written = write_to_kafka(df, bootstrap_servers, topic)

                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{current_time}] Successfully wrote {events_written} events to Kafka")

                # Calculate time to wait
                processing_time = time.time() - batch_start_time
                wait_time = max(0, batch_interval - processing_time)

                if wait_time > 0:
                    print(f"Waiting {wait_time:.2f} seconds until next batch...")
                    time.sleep(wait_time)

            except Exception as batch_error:
                print(f"Error in batch: {str(batch_error)}")
                print("Continuing with next batch after a short delay...")
                time.sleep(5)  # Wait 5 seconds before retrying

        except Exception as e:
            print(f"Error in continuous generation loop: {str(e)}")
            print("Attempting to continue operation...")
            time.sleep(10)  # Wait 10 seconds before retrying the main loop

def main():
    # Configuration
    bootstrap_servers = "osds-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
    topic = "osds-topic"
    events_per_minute = 60

    # Create Spark session using imported method
    print("Creating Spark session...")
    spark = create_spark_session()

    try:
        # Start continuous generation - this will run indefinitely
        continuous_generation(spark, bootstrap_servers, topic, events_per_minute)
    except KeyboardInterrupt:
        print("\nReceived shutdown signal...")
    except Exception as e:
        print(f"Unexpected error in main: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Stack trace:\n{traceback.format_exc()}")

    # Note: We don't stop the Spark session - it will keep running
    print("Process ended but Spark session remains active")

if __name__ == "__main__":
    main()