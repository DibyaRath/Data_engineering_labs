Scenario:

You are running a 6TB Spark job.

Pipeline:

• Read 6TB Parquet
• Join with 25GB dimension table
• GroupBy user_id
• Window aggregation
• Write to Delta

Symptoms:

• Shuffle write = 3.2TB
• Shuffle spill (disk) spikes
• Stage runs 4× slower
• Executors show high GC
• Some tasks finish in 30s, some in 40 mins


You’re asked:


1 - What triggers shuffle?
2 - What happens during Exchange?
3 - How SortMergeJoin works internally?
4 - What is Tungsten doing?
5 - When does spill happen?
6 - Why skew makes spill worse?
7 - What is ideal partition size?
8 - How does AQE adjust partitions?



# ==============================
# Spark Shuffle & Partition Tuning
# ==============================

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder \
    .appName("Shuffle_Internals_Debug") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()

# Inspect physical plan
df_join = fact_df.join(dim_df, "user_id")
df_join.explain("extended")

# Tune shuffle partitions based on data size
spark.conf.set("spark.sql.shuffle.partitions", 12000)

# Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Force broadcast if safe
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)

optimized = fact_df.join(
    broadcast(dim_df),
    "user_id"
)

optimized.write.mode("overwrite").parquet("/mnt/output/")
