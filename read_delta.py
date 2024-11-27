
from spark_config_delta import create_spark_session

# Create Spark session using the imported function
spark = create_spark_session()
spark.sql("show databases").show()


spark.sql("show databases").show()
spark.sql("select * from restaurant.k8smenu").show()
# spark.sql("select * from restaurant.orders").show()
# spark.sql("select * from restaurant.db_dictionary").show()