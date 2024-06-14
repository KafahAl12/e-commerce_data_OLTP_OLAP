from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

#Make Spark session
spark = SparkSession.builder \
    .appName("Transform Data") \
    .config("spark.jars", "/home/kafah/postgresql-42.2.26.jar") \
    .config("spark.driver.extraClassPath", "/home/kafah/postgresql-42.2.26.jar") \
    .getOrCreate()

# Read data from Parquet files
customers_df = spark.read.parquet("/home/kafah/parquet_files/customers.parquet")
products_df = spark.read.parquet("/home/kafah/parquet_files/products.parquet")
payment_methods_df = spark.read.parquet("/home/kafah/parquet_files/payment_methods.parquet")
reviews_df = spark.read.parquet("/home/kafah/parquet_files/reviews.parquet")
transactions_df = spark.read.parquet("/home/kafah/parquet_files/transactions.parquet")

# Join and transform data to create fact_transactions table
fact_df = transactions_df \
    .join(products_df, "id_produk") \
    .join(customers_df, "id_pelanggan") \
    .join(payment_methods_df.alias("pm"), transactions_df.metode_pembayaran == col("pm.id_metode")) \
    .join(reviews_df, ["id_produk", "id_pelanggan"], "left_outer") \
    .select(
        col("id_transaksi"),
        col("nama_produk"),
        col("harga"),
        col("nama_pelanggan"),
        col("email"),
        col("tanggal_transaksi"),
        col("jumlah"),
        col("total_harga"),
        col("pm.metode_pembayaran"),  
        col("status_pengiriman"),
        col("status_pembayaran"),
        when((col("rating") < 3) & (col("rating").isNotNull()), "buruk")
            .when(col("rating") >= 3, "baik")
            .otherwise("null").alias("review")
    )

# Save fact_df to Parquet file
fact_df.write.parquet("/home/kafah/parquet_files/fact_transactions.parquet", mode="overwrite")

spark.stop()
