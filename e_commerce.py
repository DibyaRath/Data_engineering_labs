Scenario:

You are a Senior Data Engineer at a global e-commerce company.

Daily pipeline:

• 5TB clickstream data
• 1.5B rows
• Incremental ingestion
• Deduplication required
• Late-arriving events
• Join with product metadata
• Compute session-level metrics
• SLA: 60 minutes
• Output must support BI + ML feature store

After traffic spike:

• Runtime increases 2.5×
• Duplicate counts inconsistent
• Incremental job reprocesses too much data
• Some sessions missing in downstream model
• Small file explosion in output

No logic change.

Input Datasets:

events(event_id string,user_id string,product_id string,event_type string,event_time timestamp,ingestion_time timestamp,session_id string,device string)

products(product_id string,category string,price double,is_active boolean,last_updated timestamp)

Expected Output:

session_metrics(session_id string,user_id string,session_start timestamp,session_end timestamp,total_events long,total_clicks long,total_value double,category_diversity int,event_date date)

🎯 Questions:

1 - How do you design idempotent incremental ingestion?
2 - How do you deduplicate late-arriving events safely?
3 - How do you watermark batch processing?
4 - How do you avoid full reprocessing daily?
5 - How do you optimize joins at 5TB scale?
6 - How do you control small file explosion?
7 - How do you detect skew dynamically?
8 - How do you enforce schema evolution safely?
9 - How do you build session metrics efficiently?
10 -How do you design output for BI + ML reuse?


1️⃣ How do you design idempotent incremental ingestion?

Use ingestion_time watermark + Delta MERGE.

Only process data greater than last_processed_ts.
Avoid reprocessing historical data.
Use MERGE to avoid duplicates on reruns.

Core idea:
Incremental logic must be deterministic.

2️⃣ How do you deduplicate late-arriving events safely?

Use window partitioned by event_id ordered by ingestion_time DESC.
Keep latest record only.

Why?

Late events may arrive after earlier versions.
We want the most recent ingestion state.

3️⃣ How do you watermark batch processing?

Even in batch:

Track max(ingestion_time) processed.
Store it externally (metadata table).
Use it as next filter boundary.

Watermark = boundary of trust.

4️⃣ How do you avoid full reprocessing daily?

Never filter by event_time.
Filter by ingestion_time.

Event time can be late.
Ingestion time reflects arrival order.

Combine with Delta MERGE to avoid overwrites.

5️⃣ How do you optimize joins at 5TB scale?

• Broadcast small dimension tables
• Enable AQE
• Use column pruning before join
• Avoid wide transformations before join

Join optimization is memory optimization.

6️⃣ How do you control small file explosion?

• Repartition by partition key
• Use maxRecordsPerFile
• Avoid excessive parallelism
• Run compaction (OPTIMIZE in Delta)

Small files = metadata overhead + slow scans.

7️⃣ How do you detect skew dynamically?

Group by key and check distribution.
Identify heavy hitters.
Salt only skewed keys.

Do not blindly salt everything.

8️⃣ How do you enforce schema evolution safely?

Enable mergeSchema in Delta.
Use append mode with autoMerge.

Do not overwrite blindly.
Allow controlled column addition.

Schema evolution must not break downstream consumers.

9️⃣ How do you build session metrics efficiently?

Reduce columns before aggregation.
Avoid unnecessary shuffle.
Pre-filter dimension data.
Aggregate after join only required columns.

Less data → less shuffle → less spill.

🔟 How do you design output for BI + ML reuse?

Partition by event_date.
Maintain deterministic schema.
Avoid overwriting historical partitions.
Use Delta versioning for reproducibility.

BI needs partition pruning.
ML needs stable features + history.

Design once. Serve many consumers.



from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Day8_Advanced_Distributed_Design") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", 4000) \
    .getOrCreate()

# ------------------------------------------
# 1️⃣ Incremental Load Based on Watermark
# ------------------------------------------

last_processed_ts = "2026-01-01 00:00:00"

events = spark.read.parquet("path/to/events") \
    .filter(col("ingestion_time") > lit(last_processed_ts))

products = spark.read.parquet("path/to/products")

# ------------------------------------------
# 2️⃣ Deduplicate Using Window (Late Events Safe)
# ------------------------------------------

dedup_window = Window.partitionBy("event_id") \
    .orderBy(col("ingestion_time").desc())

events_dedup = events.withColumn(
    "rn", row_number().over(dedup_window)
).filter(col("rn") == 1).drop("rn")

# ------------------------------------------
# 3️⃣ Optimize Join (Broadcast Small Dimension)
# ------------------------------------------

events_joined = events_dedup.join(
    broadcast(products.filter(col("is_active") == True)),
    "product_id",
    "left"
)

# ------------------------------------------
# 4️⃣ Session-Level Aggregation
# ------------------------------------------

session_window = Window.partitionBy("session_id") \
    .orderBy("event_time")

session_metrics = events_joined.groupBy(
    "session_id",
    "user_id"
).agg(
    min("event_time").alias("session_start"),
    max("event_time").alias("session_end"),
    count("*").alias("total_events"),
    sum(when(col("event_type") == "click", 1).otherwise(0)).alias("total_clicks"),
    sum("price").alias("total_value"),
    countDistinct("category").alias("category_diversity")
)

# ------------------------------------------
# 5️⃣ Partition by Event Date
# ------------------------------------------

final_df = session_metrics.withColumn(
    "event_date",
    to_date("session_start")
)

# ------------------------------------------
# 6️⃣ Detect Skew Programmatically
# ------------------------------------------

skew_check = events_dedup.groupBy("user_id").count().orderBy(desc("count"))
skew_check.show(10)

# ------------------------------------------
# 7️⃣ Small File Control
# ------------------------------------------

optimized_output = final_df.repartition("event_date")

optimized_output.write \
    .mode("overwrite") \
    .option("maxRecordsPerFile", 500000) \
    .partitionBy("event_date") \
    .parquet("path/to/session_metrics")

# ------------------------------------------
# 8️⃣ Schema Evolution Safe Write (Delta)
# ------------------------------------------

# Enable auto schema merge (if required)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

final_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("event_date") \
    .save("path/to/delta/session_metrics")


# ------------------------------------------
# 9️⃣ Efficient Session Metric Computation (Optimized Aggregation)
# ------------------------------------------

# Reduce dataset before heavy aggregation to minimize shuffle size
reduced_events = events_joined.select(
    "session_id",
    "user_id",
    "event_time",
    "event_type",
    "price",
    "category"
)

session_metrics = reduced_events.groupBy(
    "session_id",
    "user_id"
).agg(
    min("event_time").alias("session_start"),
    max("event_time").alias("session_end"),
    count("*").alias("total_events"),
    sum(when(col("event_type") == "click", 1).otherwise(0)).alias("total_clicks"),
    sum("price").alias("total_value"),
    countDistinct("category").alias("category_diversity")
)


# ------------------------------------------
# 🔟 Output Design for BI + ML Reuse
# ------------------------------------------

final_df = session_metrics.withColumn(
    "event_date",
    to_date("session_start")
)

# Write partitioned output for BI query pruning
final_df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .save("path/to/delta/session_metrics")

# Optional: Optimize layout for analytics (Databricks)
# spark.sql("OPTIMIZE delta.`path/to/delta/session_metrics` ZORDER BY (user_id, session_id)")
