You are a Data Engineer at a global fintech company. You Work in a computation pipeline.

Pipeline Scale:

• 5 TB daily input
• 150K events/sec peak
• Join with 18 GB reference table
• 7-day rolling aggregations
• Writes to Delta Lake
• Downstream ML depends on it
• SLA: 40 minutes

Suddenly:

• Runtime increases 5x
• Shuffle spill jumps 12x
• 4 tasks run 30x longer than others
• Executor memory pressure spikes
• No code was deployed

Yesterday: 32 minutes.
Today: 2 hours 47 minutes.

Same code.
Same cluster.
Same dataset.


events_df:
(user_id, event_timestamp, event_date, amount, device_type, country)

user_profile_df:
(user_id, user_segment, risk_score, account_status)

Join key → user_id
Partition key → event_date

🎯 Questions

1 - How do you detect skew programmatically?
2 - How do you inspect physical plan?
3 - Did Spark change join strategy?
4 - How do you enforce deterministic join behavior?
5 - How do you isolate heavy keys?
6 - How do you prevent state explosion?
7 - How do you scale this 3× safely?

# ==============================
# FAANG L5/L6 – Distributed Debugging & Optimization Toolkit
# ==============================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, desc, broadcast, rand, lit, window
)

spark = SparkSession.builder \
    .appName("FAANG_L5_L6_Optimization") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()

# ==========================================
# 1️⃣ Detect Heavy-Key Skew
# ==========================================

skew_df = (
    events_df
    .groupBy("user_id")
    .count()
    .orderBy(desc("count"))
)

skew_df.show(20)

total_records = events_df.count()

top_volume = (
    skew_df
    .limit(10)
    .selectExpr("sum(count) as top_volume")
    .collect()[0][0]
)

print("Top user traffic ratio:", top_volume / total_records)

# ==========================================
# 2️⃣ Inspect Physical Plan
# ==========================================

events_df.join(user_profile_df, "user_id").explain("extended")

# ==========================================
# 3️⃣ Enforce Deterministic Join Strategy
# ==========================================

spark.conf.set(
    "spark.sql.autoBroadcastJoinThreshold",
    100 * 1024 * 1024
)

optimized_join = events_df.join(
    broadcast(user_profile_df),
    "user_id"
)

# ==========================================
# 4️⃣ Heavy-Key Isolation via Salting
# ==========================================

salted_events = events_df.withColumn(
    "salt",
    (rand() * 10).cast("int")
)

salted_profiles = user_profile_df.withColumn(
    "salt",
    lit(0)
)

salted_join = salted_events.join(
    salted_profiles,
    ["user_id", "salt"]
)

# ==========================================
# 5️⃣ Streaming 7-Day Window with Watermark
# ==========================================

aggregated_stream = (
    stream_df
    .withWatermark("event_timestamp", "1 day")
    .groupBy(
        col("user_id"),
        window(col("event_timestamp"), "7 days", "1 day")
    )
    .count()
)

# ==========================================
# 6️⃣ Shuffle Partition Tuning
# ==========================================

spark.conf.set("spark.sql.shuffle.partitions", 2000)

# ==========================================
# 7️⃣ Memory & Spill Awareness
# ==========================================

spark.conf.set("spark.memory.fraction", 0.6)
spark.conf.set("spark.sql.shuffle.spill", "true")

# ==========================================
# 8️⃣ Write Optimized Output
# ==========================================

optimized_join.repartition("event_date") \
    .write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet("/mnt/output/")
