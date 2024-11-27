from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *



spark = SparkSession.builder \
    .appName("Hudi Basics") \
    .config("spark.jars.packages", 'org.apache.hudi:hudi-spark3.5-bundle_2.12:0.14.1') \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext


# pyspark
tripsDF = spark.read.format("hudi").load("gs://osd-data/source/menu_items.csv")
tripsDF.createOrReplaceTempView("trips_table")

spark.sql("SELECT * from trips_table").show()