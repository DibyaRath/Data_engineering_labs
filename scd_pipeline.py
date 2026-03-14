from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("scd_type2_pipeline") \
    .config("spark.sql.adaptive.enabled","true") \
    .config("spark.sql.shuffle.partitions","800") \
    .getOrCreate()

# Read updates
updates = spark.read.format("parquet") \
    .load("/data/customer_updates")

# Generate attribute hash
updates = updates.withColumn(
    "attr_hash",
    sha2(concat_ws("||",
        col("name"),
        col("email"),
        col("city"),
        col("loyalty_tier")),256)
)

# Load dimension table
dim_table = DeltaTable.forPath(
    spark,
    "/delta/customer_dim"
)

dim_df = dim_table.toDF()

# Filter current records
current_dim = dim_df.filter(col("is_current") == True)

current_dim = current_dim.withColumn(
    "attr_hash",
    sha2(concat_ws("||",
        col("name"),
        col("email"),
        col("city"),
        col("loyalty_tier")),256)
)

# Detect changed records
changes = updates.alias("u") \
    .join(current_dim.alias("d"),"customer_id") \
    .filter(col("u.attr_hash") != col("d.attr_hash"))

# Close previous record
dim_table.alias("target").merge(
    changes.alias("source"),
    "target.customer_id = source.customer_id \
     AND target.is_current = true"
).whenMatchedUpdate(set={
    "effective_to": "source.updated_at",
    "is_current": "false"
}).execute()

# Insert new version
new_versions = changes.select(
    col("customer_id"),
    col("name"),
    col("email"),
    col("city"),
    col("loyalty_tier"),
    col("updated_at").alias("effective_from")
).withColumn(
    "effective_to",
    lit(None).cast("timestamp")
).withColumn(
    "is_current",
    lit(True)
)

new_versions.write \
    .format("delta") \
    .mode("append") \
    .save("/delta/customer_dim")

# Validation
validation = spark.read.format("delta") \
    .load("/delta/customer_dim") \
    .groupBy("customer_id") \
    .agg(sum(col("is_current").cast("int")).alias("active_records")) \
    .filter(col("active_records") > 1)

if validation.count() > 0:
    raise Exception("Multiple active SCD records detected")

print("SCD pipeline completed successfully")