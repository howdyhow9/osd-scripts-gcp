from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def create_spark_session():
    builder = SparkSession.builder \
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

return configure_spark_with_delta_pip(builder).getOrCreate()
