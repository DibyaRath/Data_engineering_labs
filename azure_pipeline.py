from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Read bronze data
df_bronze = spark.read.format("parquet") \
    .load("abfss://bronze@datalake/data")

# Clean and transform (Silver layer)
df_silver = (
    df_bronze
    .filter(F.col("price").isNotNull())
    .withColumn("revenue", F.col("price") * F.col("units_sold"))
)

# Write Silver data
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://silver@datalake/sales_clean")

# Load Gold dimension table
gold_path = "abfss://gold@datalake/dim_product"

if DeltaTable.isDeltaTable(spark, gold_path):

    delta_table = DeltaTable.forPath(spark, gold_path)

    # Incremental merge
    (
        delta_table.alias("target")
        .merge(
            df_silver.alias("source"),
            "target.product_id = source.product_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_path)