# ==========================================================
# Day 9 – Large Scale SCD Type 2 + Late Data + Idempotency
# ==========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("Day9_SCD_Type2_Production") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", 4000) \
    .getOrCreate()

# ----------------------------------------------------------
# 1️⃣ Load Incremental Updates
# ----------------------------------------------------------

watermark_ts = "2026-01-01 00:00:00"

updates = spark.read.parquet("path/to/customer_updates") \
    .filter(col("ingestion_time") > lit(watermark_ts))

# ----------------------------------------------------------
# 2️⃣ Deduplicate Late Records
# ----------------------------------------------------------

dedup_window = Window.partitionBy("customer_id") \
    .orderBy(col("updated_at").desc(), col("ingestion_time").desc())

updates_dedup = updates.withColumn(
    "rn", row_number().over(dedup_window)
).filter(col("rn") == 1).drop("rn")

# ----------------------------------------------------------
# 3️⃣ Create Change Hash
# ----------------------------------------------------------

updates_hashed = updates_dedup.withColumn(
    "record_hash",
    sha2(concat_ws("||",
        col("name"),
        col("email"),
        col("address"),
        col("risk_level")
    ), 256)
)

# ----------------------------------------------------------
# 4️⃣ Load Existing Dimension
# ----------------------------------------------------------

dim_path = "path/to/delta/customer_dim"

if DeltaTable.isDeltaTable(spark, dim_path):
    dim_table = DeltaTable.forPath(spark, dim_path)
    dim_df = dim_table.toDF()
else:
    updates_hashed.withColumn("effective_from", col("updated_at")) \
        .withColumn("effective_to", lit(None).cast("timestamp")) \
        .withColumn("is_current", lit(True)) \
        .write.format("delta").save(dim_path)
    exit()

# ----------------------------------------------------------
# 5️⃣ Compare Only Current Records
# ----------------------------------------------------------

current_dim = dim_df.filter(col("is_current") == True)

current_dim_hashed = current_dim.withColumn(
    "record_hash",
    sha2(concat_ws("||",
        col("name"),
        col("email"),
        col("address"),
        col("risk_level")
    ), 256)
)

changes = updates_hashed.alias("u").join(
    current_dim_hashed.alias("d"),
    "customer_id",
    "left"
).filter(
    (col("d.record_hash").isNull()) |
    (col("u.record_hash") != col("d.record_hash"))
)

# ----------------------------------------------------------
# 6️⃣ Close Old Versions
# ----------------------------------------------------------

closing_records = changes.select(
    col("customer_id"),
    col("updated_at").alias("new_effective_from")
)

dim_table.alias("d").merge(
    closing_records.alias("c"),
    "d.customer_id = c.customer_id AND d.is_current = true"
).whenMatchedUpdate(
    set={
        "effective_to": col("c.new_effective_from"),
        "is_current": lit(False)
    }
).execute()

# ----------------------------------------------------------
# 7️⃣ Insert New Versions
# ----------------------------------------------------------

new_versions = changes.select(
    "customer_id",
    "name",
    "email",
    "address",
    "risk_level",
    col("updated_at").alias("effective_from")
).withColumn("effective_to", lit(None).cast("timestamp")) \
 .withColumn("is_current", lit(True))

new_versions.write.format("delta") \
    .mode("append") \
    .save(dim_path)

# ----------------------------------------------------------
# 8️⃣ Validate No Overlapping History
# ----------------------------------------------------------

window_check = Window.partitionBy("customer_id").orderBy("effective_from")

overlap_check = dim_table.toDF().withColumn(
    "next_start",
    lead("effective_from").over(window_check)
).filter(
    col("effective_to").isNotNull() &
    (col("effective_to") > col("next_start"))
)

if overlap_check.count() > 0:
    print("WARNING: Overlapping intervals detected")

# ----------------------------------------------------------
# 9️⃣ Optimize for Scale
# ----------------------------------------------------------

spark.sql(f"OPTIMIZE delta.`{dim_path}` ZORDER BY (customer_id)")

# ----------------------------------------------------------
# 🔟 Point-in-Time Query Example
# ----------------------------------------------------------

as_of_time = "2026-02-01 00:00:00"

point_in_time_df = dim_table.toDF().filter(
    (col("effective_from") <= lit(as_of_time)) &
    (
        col("effective_to").isNull() |
        (col("effective_to") > lit(as_of_time))
    )
)

point_in_time_df.show()
