# ==========================================================
# COMPLETE PARTITION OPTIMIZATION DEMO - PRODUCTION 
# Covers:
# 1. Dataset creation
# 2. Skew simulation
# 3. Partition diagnostics
# 4. Repartition
# 5. Coalesce
# 6. Shuffle partition tuning
# 7. Adaptive Query Execution
# 8. maxPartitionBytes
# 9. Salting to fix skew
# 10. maxRecordsPerFile during write
# ==========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import random

# ----------------------------------------------------------
# 1️⃣ Spark Session with Proper Config
# ----------------------------------------------------------

spark = SparkSession.builder \
    .appName("FullPartitionOptimizationDemo") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "300") \
    .config("spark.sql.files.maxPartitionBytes", 134217728) \  # 128MB
    .getOrCreate()

print("Adaptive Enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
print("Shuffle Partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# ----------------------------------------------------------
# 2️⃣ Create Large Input Dataset (10M Rows)
# ----------------------------------------------------------

df = spark.range(0, 10_000_000) \
    .withColumn("value", rand()) \
    .withColumn("category",
        when(col("id") % 100 < 90, "A")  # 90% skew
        .otherwise("B")
    )

print("Initial Partition Count:", df.rdd.getNumPartitions())

# ----------------------------------------------------------
# 3️⃣ Detect Skew (Before Fix)
# ----------------------------------------------------------

df.groupBy("category").count().show()

# ----------------------------------------------------------
# 4️⃣ Repartition (Increase Parallelism)
# ----------------------------------------------------------

df_repartitioned = df.repartition(200)
print("After Repartition:", df_repartitioned.rdd.getNumPartitions())

# ----------------------------------------------------------
# 5️⃣ Coalesce (Reduce Small Partitions)
# ----------------------------------------------------------

df_coalesced = df_repartitioned.coalesce(100)
print("After Coalesce:", df_coalesced.rdd.getNumPartitions())

# ----------------------------------------------------------
# 6️⃣ Shuffle Operation (Join to Simulate Heavy Workload)
# ----------------------------------------------------------

df2 = spark.range(0, 10_000_000) \
    .withColumn("value2", rand())

joined_df = df_coalesced.join(df2, "id")

print("Post Join Partition Count:", joined_df.rdd.getNumPartitions())

# ----------------------------------------------------------
# 7️⃣ Fix Skew Using Salting
# ----------------------------------------------------------

salted_df = df_coalesced.withColumn(
    "salt", floor(rand() * 10)
)

df2_salted = df2.withColumn(
    "salt", floor(rand() * 10)
)

salted_join = salted_df.join(df2_salted, ["id", "salt"])

print("Salted Join Partition Count:", salted_join.rdd.getNumPartitions())

# ----------------------------------------------------------
# 8️⃣ Write Optimized Output (Avoid Small Files)
# ----------------------------------------------------------

salted_join.write \
    .option("maxRecordsPerFile", 500000) \
    .mode("overwrite") \
    .parquet("/tmp/optimized_output")

print("Write Completed Successfully")

# ----------------------------------------------------------
# 9️⃣ Final Diagnostics
# ----------------------------------------------------------

print("Final Shuffle Partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
print("AQE Enabled:", spark.conf.get("spark.sql.adaptive.enabled"))

spark.stop()

# ==========================================================
# END OF SCRIPT
# ==========================================================
