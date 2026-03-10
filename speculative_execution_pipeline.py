from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder \
    .appName("speculative_execution_pipeline") \
    .getOrCreate()

# Enable speculative execution
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.speculation.multiplier", "1.5")
spark.conf.set("spark.speculation.quantile", "0.75")
spark.conf.set("spark.speculation.minTaskRuntime", "100")

# Read datasets
transactions = spark.read.parquet("/data/transactions")
customers = spark.read.parquet("/data/customers")
products = spark.read.parquet("/data/products")

# Join large datasets
df = transactions.join(customers, "customer_id") \
                 .join(products, "product_id")

# Aggregation
regional_sales = df.groupBy("region") \
    .agg(sum(col("transaction_amount")).alias("total_sales"))

# Write result
regional_sales.write \
    .mode("overwrite") \
    .format("delta") \
    .save("/delta/regional_sales")

print("Speculative execution enabled:",
      spark.conf.get("spark.speculation"))