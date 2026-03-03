📥 Input Dataset

customer_updates

customer_id STRING

risk_score INT

status STRING

country STRING

kyc_level STRING

event_time TIMESTAMP

ingestion_time TIMESTAMP

📤 Target Table (SCD Type 2)

customer_dim

customer_id STRING

risk_score INT

status STRING

country STRING

kyc_level STRING

effective_from TIMESTAMP

effective_to TIMESTAMP

is_current BOOLEAN


# =========================
# SCD TYPE 2 AT 500M SCALE
# =========================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("SCD_Type2_500M").getOrCreate()

# ---------------------------------
# 1. Load Source Updates
# ---------------------------------
updates_df = spark.read.format("delta").load("/mnt/data/customer_updates")

# Deduplicate for idempotency
updates_df = updates_df.dropDuplicates(["customer_id", "event_time"])

# ---------------------------------
# 2. Add Hash For Change Detection
# ---------------------------------
tracked_cols = ["risk_score", "status", "country", "kyc_level"]

updates_df = updates_df.withColumn(
    "hash_value",
    sha2(concat_ws("||", *tracked_cols), 256)
)

# ---------------------------------
# 3. Load Target Active Slice Only
# ---------------------------------
dim_df = spark.read.format("delta").load("/mnt/data/customer_dim")

active_df = dim_df.filter(col("is_current") == True)

active_df = active_df.withColumn(
    "hash_value",
    sha2(concat_ws("||", *tracked_cols), 256)
)

# ---------------------------------
# 4. Identify Changed Records Only
# ---------------------------------
changes_df = updates_df.alias("s").join(
    active_df.alias("t"),
    "customer_id",
    "left"
).filter(
    (col("t.hash_value").isNull()) |
    (col("s.hash_value") != col("t.hash_value"))
)

# ---------------------------------
# 5. Handle Late Arrivals (Interval Logic)
# ---------------------------------
late_updates = changes_df.filter(
    col("s.event_time") < col("t.effective_from")
)

# ---------------------------------
# 6. Perform Delta MERGE
# ---------------------------------
delta_table = DeltaTable.forPath(spark, "/mnt/data/customer_dim")

delta_table.alias("t").merge(
    changes_df.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true"
).whenMatchedUpdate(
    condition="s.hash_value != t.hash_value",
    set={
        "is_current": "false",
        "effective_to": "s.event_time"
    }
).whenNotMatchedInsert(
    values={
        "customer_id": "s.customer_id",
        "risk_score": "s.risk_score",
        "status": "s.status",
        "country": "s.country",
        "kyc_level": "s.kyc_level",
        "effective_from": "s.event_time",
        "effective_to": "null",
        "is_current": "true"
    }
).execute()

# ---------------------------------
# 7. Validate No Duplicate Active Records
# ---------------------------------
validation_df = spark.read.format("delta").load("/mnt/data/customer_dim")

duplicates = validation_df.groupBy("customer_id") \
    .agg(sum(col("is_current").cast("int")).alias("active_count")) \
    .filter(col("active_count") > 1)

if duplicates.count() > 0:
    raise Exception("Duplicate active records detected!")

# ---------------------------------
# 8. Validate Temporal Correctness
# ---------------------------------
window_spec = Window.partitionBy("customer_id").orderBy("effective_from")

interval_check = validation_df.withColumn(
    "next_start",
    lead("effective_from").over(window_spec)
).filter(
    col("effective_to") > col("next_start")
)

if interval_check.count() > 0:
    raise Exception("Overlapping intervals detected!")

# ---------------------------------
# 9. Enable Point-in-Time Query Example
# ---------------------------------
as_of_time = "2026-03-01"

point_in_time_df = validation_df.filter(
    (col("effective_from") <= as_of_time) &
    ((col("effective_to").isNull()) | (col("effective_to") > as_of_time))
)

# ---------------------------------
# 10. Optimize Table Layout
# ---------------------------------
spark.sql("OPTIMIZE customer_dim ZORDER BY (customer_id)")
