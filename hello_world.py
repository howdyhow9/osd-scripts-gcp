from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("HelloSpark") \
    .getOrCreate()

# Create a DataFrame with a single column and a single row containing "Hello, World!"
data = [("Hello, World!",)]
columns = ["greeting"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Stop the Spark session
spark.stop()
