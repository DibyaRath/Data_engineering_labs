You are a Senior Data Engineer at a ride-sharing company.

You receive ~500GB of trip data daily in Parquet format.

Schema:

- trip_id (string)
- driver_id (string)
- city (string)
- fare_amount (double)
- trip_distance (double)
- trip_timestamp (timestamp)

Business Requirements:

1. Identify the Top 3 drivers per city per day based on total fare.
2. Data may arrive late and may contain duplicate records.
3. Some cities generate 10x more data than others (data skew).
4. Output must support fast downstream analytical queries.
5. The solution must scale reliably as data grows.

How would you implement this in PySpark?


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("RideSharingDriverRanking") \
    .getOrCreate()

# 1. Load Parquet Data (Columnar format for performance)
df = spark.read.parquet("path/to/trip_data/")

# 2. Remove Duplicates (Idempotent processing)
df_dedup = df.dropDuplicates(["trip_id"])

# 3. Extract Trip Date
df_with_date = df_dedup.withColumn(
    "trip_date",
    to_date(col("trip_timestamp"))
)

# 4. Aggregate Total Fare per Driver per City per Day
driver_daily = df_with_date.groupBy(
    "city", "trip_date", "driver_id"
).agg(
    sum("fare_amount").alias("total_fare")
)

# 5. Rank Drivers per City per Day
window_spec = Window.partitionBy(
    "city", "trip_date"
).orderBy(col("total_fare").desc())

ranked_drivers = driver_daily.withColumn(
    "rank",
    row_number().over(window_spec)
)

# 6. Filter Top 3
top_drivers = ranked_drivers.filter(col("rank") <= 3)

# 7. Optimized Write
top_drivers \
    .repartition("trip_date") \
    .write \
    .mode("overwrite") \
    .partitionBy("trip_date") \
    .parquet("path/to/output/")
