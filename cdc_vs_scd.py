
You are designing a **customer data platform**.

Requirements:

• Capture every change from source systems
• Maintain full historical data
• Support dashboards + analytics
• Scale to **10B+ records/day**

Now the real question is:

👉 **Do you use CDC or SCD?**

---

### 🔄 CDC — Change Data Capture

CDC tracks **what changed in the system**.

Every event is captured:

👇

## customer_id   city        operation   timestamp

101           Mumbai      INSERT      10:00
101           Bangalore   UPDATE      12:00
101           Bangalore   DELETE      15:00

👉 CDC = **event stream of changes**

Used for:

• real-time pipelines
• streaming ingestion
• syncing databases

---

### 🕰️ SCD — Slowly Changing Dimensions

SCD tracks **how data looked over time**.

👇

## customer_id   city        start_date   end_date   is_current

101           Mumbai      10:00        12:00      false
101           Bangalore   12:00        NULL       true

👉 SCD = **historical state**

Used for:

• data warehouse
• BI reporting
• point-in-time analysis


from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# =========================================
# STEP 1: READ CDC EVENTS (BRONZE)
# =========================================

df_cdc = spark.read.format("delta").load("/bronze/cdc_events")

# Expected columns:
# customer_id, city, operation, event_time

# =========================================
# STEP 2: DEDUPLICATION (KEEP LATEST EVENT)
# =========================================

window_spec = Window.partitionBy("customer_id") \
                   .orderBy(F.col("event_time").desc())

df_dedup = (
    df_cdc.withColumn("rn", F.row_number().over(window_spec))
          .filter("rn = 1")
          .drop("rn")
)

# =========================================
# STEP 3: FILTER VALID CDC OPERATIONS
# =========================================

df_filtered = df_dedup.filter(
    F.col("operation").isin("INSERT", "UPDATE", "DELETE")
)

# =========================================
# STEP 4: HASH FOR CHANGE DETECTION
# =========================================

df_enriched = df_filtered.withColumn(
    "hash_value",
    F.sha2(F.concat_ws("||", "customer_id", "city"), 256)
)

# =========================================
# STEP 5: CDC → SILVER (LATEST STATE TABLE)
# =========================================

silver_path = "/silver/customer_latest"

if DeltaTable.isDeltaTable(spark, silver_path):

    silver_table = DeltaTable.forPath(spark, silver_path)

    (
        silver_table.alias("target")
        .merge(
            df_enriched.alias("source"),
            "target.customer_id = source.customer_id"
        )
        # UPDATE existing
        .whenMatchedUpdate(
            condition="source.operation != 'DELETE'",
            set={
                "city": "source.city",
                "hash_value": "source.hash_value",
                "updated_at": "source.event_time"
            }
        )
        # DELETE records
        .whenMatchedDelete(condition="source.operation = 'DELETE'")
        # INSERT new
        .whenNotMatchedInsert(
            condition="source.operation != 'DELETE'",
            values={
                "customer_id": "source.customer_id",
                "city": "source.city",
                "hash_value": "source.hash_value",
                "updated_at": "source.event_time"
            }
        )
        .execute()
    )

else:
    df_enriched.write.format("delta").mode("overwrite").save(silver_path)

# =========================================
# STEP 6: PREPARE SCD SOURCE DATA
# =========================================

df_silver = spark.read.format("delta").load(silver_path)

# Add SCD columns
df_scd_source = (
    df_silver
    .withColumn("start_date", F.col("updated_at"))
    .withColumn("end_date", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
)

# =========================================
# STEP 7: SCD TYPE 2 → GOLD LAYER
# =========================================

gold_path = "/gold/dim_customer"

if DeltaTable.isDeltaTable(spark, gold_path):

    gold_table = DeltaTable.forPath(spark, gold_path)

    # -------------------------------------
    # STEP 7A: CLOSE OLD RECORDS
    # -------------------------------------

    (
        gold_table.alias("target")
        .merge(
            df_scd_source.alias("source"),
            """
            target.customer_id = source.customer_id
            AND target.is_current = true
            """
        )
        .whenMatchedUpdate(
            condition="""
            target.hash_value != source.hash_value
            AND source.updated_at >= target.start_date
            """,
            set={
                "end_date": "source.updated_at",
                "is_current": "false"
            }
        )
        .execute()
    )

    # -------------------------------------
    # STEP 7B: INSERT NEW RECORDS
    # -------------------------------------

    df_new_records = df_scd_source.alias("source").join(
        gold_table.toDF().alias("target"),
        "customer_id",
        "left"
    ).filter(
        (F.col("target.customer_id").isNull()) |  # new record
        (F.col("target.hash_value") != F.col("source.hash_value"))  # changed
    )

    df_new_records.select(
        "customer_id",
        "city",
        "start_date",
        "end_date",
        "is_current",
        "hash_value"
    ).write.format("delta").mode("append").save(gold_path)

else:

    df_scd_source.select(
        "customer_id",
        "city",
        "start_date",
        "end_date",
        "is_current",
        "hash_value"
    ).write.format("delta").mode("overwrite").save(gold_path)

# =========================================
# STEP 8: OPTIONAL — HANDLE LATE ARRIVING DATA
# =========================================

# Identify late data (example: older than 1 day)
df_late = df_enriched.filter(
    F.col("event_time") < F.current_timestamp() - F.expr("INTERVAL 1 DAY")
)

