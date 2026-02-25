💡 Day 6 – PySpark Scenario-Based Interview Question

One partition working 30× harder than the rest — that’s how jobs collapse.

Your cluster looks healthy.
CPU is fine.
Memory is fine.

Yet one task keeps running.

Stage stuck at 99%.

This is how data skew kills distributed systems.

Scenario:

You are a Senior Data Engineer at a global e-commerce company.

• 4TB fact table
• 22GB dimension table
• Join on user_id
• Aggregation after join
• SLA: 40 minutes

After one week:

• Runtime jumps 28 min → 2 hours
• Few tasks run 30× longer
• Shuffle spill spikes
• High GC
• No code changes

Questions:

1 - Why are only a few tasks slow?
2 - What is happening inside shuffle?
3 - Did Spark change join strategy?
4 - How do you prove skew?
5 - How do you fix it safely?
6 - How do you prevent recurrence?

Answers 
1️⃣ Why Are Only a Few Tasks Slow?

Because partition sizes are uneven.

Spark distributes data by hash(user_id).
If some user_ids dominate volume → one partition becomes huge.

That task:

• Processes more data
• Spills more
• Runs longer

Parallel system becomes partially serialized.

2️⃣ What Is Happening Inside Shuffle?

During SortMergeJoin:

• Map side writes shuffle files
• Data hashed into partitions
• Reducers fetch relevant partitions
• Sorting happens in memory

If one partition is massive → spill → disk I/O → GC pressure → slowdown.

Inspect plan:

fact_df.join(dim_df, "user_id").explain("extended")

Look for:
SortMergeJoin + Exchange operators.

3️⃣ Did Spark Change Join Strategy?

Possibly.

If dimension table grew beyond broadcast threshold,
Spark switches from BroadcastHashJoin → SortMergeJoin.

Check threshold:

spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

Force broadcast (if safe):

from pyspark.sql.functions import broadcast

fact_df.join(broadcast(dim_df), "user_id")
4️⃣ How Do You Prove Skew?

Measure key distribution.

from pyspark.sql.functions import desc

fact_df.groupBy("user_id") \
    .count() \
    .orderBy(desc("count")) \
    .show(20)

If top keys dominate → skew confirmed.

Also inspect Spark UI:
Uneven task durations = skew symptom.

5️⃣ How Do You Fix It Safely?

Option A: Enable AQE

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

Option B: Salting

from pyspark.sql.functions import rand, lit

salted_fact = fact_df.withColumn("salt", (rand()*10).cast("int"))
salted_dim = dim_df.withColumn("salt", lit(0))

salted_fact.join(salted_dim, ["user_id", "salt"])

Option C: Pre-aggregate before join.

6️⃣ How Do You Prevent Recurrence?

• Monitor skew metrics in production
• Track top key distribution
• Partition based on composite keys
• Tune shuffle partitions properly

spark.conf.set("spark.sql.shuffle.partitions", 4000)

Partition math:
Shuffle size ÷ 256MB ≈ partition count.

Skew prevention is architectural, not reactive.


# ==========================================
# Day 6 – Data Skew Detection & Optimization
# ==========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, broadcast, rand, lit

# ------------------------------------------
# Create Spark Session
# ------------------------------------------
spark = SparkSession.builder \
    .appName("Day6_DataSkew_Optimization") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()

# ------------------------------------------
# Example DataFrames (Replace with actual sources)
# ------------------------------------------
fact_df = spark.read.parquet("path/to/fact_table")
dim_df = spark.read.parquet("path/to/dimension_table")

# ==========================================
# 1️⃣ Detect Data Skew
# ==========================================

skew_df = (
    fact_df
    .groupBy("user_id")
    .count()
    .orderBy(desc("count"))
)

skew_df.show(20)

# ==========================================
# 2️⃣ Inspect Physical Plan
# ==========================================

fact_df.join(dim_df, "user_id").explain("extended")

# ==========================================
# 3️⃣ Check / Adjust Broadcast Threshold
# ==========================================

print("Current Broadcast Threshold:",
      spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

spark.conf.set(
    "spark.sql.autoBroadcastJoinThreshold",
    100 * 1024 * 1024  # 100MB
)

# Force Broadcast Join (if safe)
optimized_join = fact_df.join(
    broadcast(dim_df),
    "user_id"
)

# ==========================================
# 4️⃣ Enable Adaptive Query Execution (AQE)
# ==========================================

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# ==========================================
# 5️⃣ Salting Technique for Heavy Keys
# ==========================================

salted_fact = fact_df.withColumn(
    "salt",
    (rand() * 10).cast("int")
)

salted_dim = dim_df.withColumn(
    "salt",
    lit(0)
)

salted_join = salted_fact.join(
    salted_dim,
    ["user_id", "salt"]
)

# ==========================================
# 6️⃣ Tune Shuffle Partitions
# ==========================================

spark.conf.set("spark.sql.shuffle.partitions", 4000)

# ==========================================
# 7️⃣ Write Optimized Output
# ==========================================

optimized_join.repartition("event_date") \
    .write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet("path/to/output/")


      
