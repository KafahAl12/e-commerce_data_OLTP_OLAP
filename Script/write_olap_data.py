from pyspark.sql import SparkSession
import os

# Make Spark session
spark = SparkSession.builder \
    .appName("Write Data") \
    .config("spark.jars", "/home/kafah/postgresql-42.2.26.jar") \
    .config("spark.driver.extraClassPath", "/home/kafah/postgresql-42.2.26.jar") \
    .getOrCreate()

# Get JDBC URLs and properties from environment variables
jdbc_url_olap = os.getenv("JDBC_URL_OLAP")
properties = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

# Read data from Parquet files
customers_df = spark.read.parquet("/home/kafah/parquet_files/customers.parquet")
products_df = spark.read.parquet("/home/kafah/parquet_files/products.parquet")
payment_methods_df = spark.read.parquet("/home/kafah/parquet_files/payment_methods.parquet")
fact_df = spark.read.parquet("/home/kafah/parquet_files/fact_transactions.parquet")

# Write fact_transactions to OLAP database
fact_df.write.jdbc(jdbc_url_olap, "fact_transactions", mode="overwrite", properties=properties)

# Write dimension tables to OLAP database
customers_df.write.jdbc(jdbc_url_olap, "dim_customers", mode="overwrite", properties=properties)
products_df.write.jdbc(jdbc_url_olap, "dim_products", mode="overwrite", properties=properties)
payment_methods_df.write.jdbc(jdbc_url_olap, "dim_payment_methods", mode="overwrite", properties=properties)

spark.stop()
