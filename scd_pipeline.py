Scenario:

You are a Senior Data Engineer at a global retail company.

Your team maintains a **customer dimension table** used for marketing analytics and fraud detection.

Scale:

• 1B customers
• 2.8B historical records
• 1.5 TB daily updates

Cluster:

• 200 executors
• 8 cores each

SLA: **60 minutes**


Input Dataset : customer_updates

customer_id
name
email
city
loyalty_tier
updated_at
ingestion_time

Target Table (SCD Type 2) : customer_dim

customer_id
name
email
city
loyalty_tier
effective_from
effective_to
is_current



1️⃣ Write PySpark code to read incremental customer updates from Parquet.
2️⃣ Write PySpark code to generate a hash column for change detection.
3️⃣ Write PySpark code to load an existing Delta Lake dimension table.
4️⃣ Write PySpark code to filter only current records.
5️⃣ Write PySpark code to detect changed rows.
6️⃣ Write PySpark code to close the previous SCD record using Delta MERGE.
7️⃣ Write PySpark code to insert the new SCD version.
8️⃣ Write PySpark code to validate that only one active record exists.



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
