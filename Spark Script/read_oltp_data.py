from pyspark.sql import SparkSession
import os

# Make Spark Session
spark = SparkSession.builder \
    .appName("Read Data") \
    .config("spark.jars", "/home/kafah/postgresql-42.2.26.jar") \
    .config("spark.driver.extraClassPath", "/home/kafah/postgresql-42.2.26.jar") \
    .getOrCreate()

# GeT JDBC URLs and properties from environment variables
jdbc_url_oltp = os.getenv("JDBC_URL_OLTP")
properties = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

# Load data from OLTP database using Spark read
customers_df = spark.read.jdbc(jdbc_url_oltp, "customers", properties=properties)
products_df = spark.read.jdbc(jdbc_url_oltp, "products", properties=properties)
payment_methods_df = spark.read.jdbc(jdbc_url_oltp, "payment_methods", properties=properties)
reviews_df = spark.read.jdbc(jdbc_url_oltp, "reviews", properties=properties)
transactions_df = spark.read.jdbc(jdbc_url_oltp, "transactions", properties=properties)

# Save data to Parquet files
customers_df.write.parquet("/home/kafah/parquet_files/customers.parquet", mode="overwrite")
products_df.write.parquet("/home/kafah/parquet_files/products.parquet", mode="overwrite")
payment_methods_df.write.parquet("/home/kafah/parquet_files/payment_methods.parquet", mode="overwrite")
reviews_df.write.parquet("/home/kafah/parquet_files/reviews.parquet", mode="overwrite")
transactions_df.write.parquet("/home/kafah/parquet_files/transactions.parquet", mode="overwrite")

spark.stop()
