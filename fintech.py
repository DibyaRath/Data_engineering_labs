You are working as a Data Engineer in a fintech company.

You receive daily transaction logs (CSV) with:

- transaction_id (string)
- user_id (string)
- amount (double)
- transaction_type (string: credit/debit)
- transaction_timestamp (string)

You are asked to:

1. Load the CSV into PySpark with an explicit schema
2. Remove duplicate transactions
3. Filter out records where amount <= 0
4. Convert transaction_timestamp to proper timestamp format
5. Calculate total debit and credit amount per day
6. Store the result in Parquet format partitioned by date

Snippet : 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create Spark Session
spark = SparkSession.builder \
    .appName("FintechTransactionPipeline") \
    .getOrCreate()

# Define Explicit Schema (Production Best Practice)
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("transaction_timestamp", StringType(), True)
])

# 1. Load CSV
df = spark.read \
    .option("header", True) \
    .schema(schema) \
    .csv("path/to/transactions.csv")

# 2. Remove Duplicates
df_dedup = df.dropDuplicates(["transaction_id"])

# 3. Filter Invalid Records
df_valid = df_dedup.filter(col("amount") > 0)

# 4. Convert Timestamp & Extract Date
df_transformed = df_valid.withColumn(
    "transaction_ts",
    to_timestamp(col("transaction_timestamp"))
).withColumn(
    "transaction_date",
    to_date(col("transaction_ts"))
)

# 5. Aggregate Debit & Credit Per Day
daily_summary = df_transformed.groupBy(
    "transaction_date", "transaction_type"
).agg(
    sum("amount").alias("total_amount")
)

# 6. Write to Parquet Partitioned by Date
daily_summary.write \
    .mode("overwrite") \
    .partitionBy("transaction_date") \
    .parquet("path/to/output/")
