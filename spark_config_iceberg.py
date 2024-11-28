from pyspark.sql import SparkSession

def create_spark_session():
    builder = SparkSession.builder \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hive") \
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
        .config("spark.hadoop.fs.gs.project.id", "osd-k8s") \
        .config("spark.hadoop.fs.gs.system.bucket", "osd-data") \
        .config("iceberg.engine.hive.lock-enabled", "false") \
        .enableHiveSupport()

    return builder.getOrCreate()

