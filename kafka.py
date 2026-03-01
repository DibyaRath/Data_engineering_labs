💡 Day 11 – PySpark Scenario-Based Interview Question

Your streaming job didn’t crash.
It silently corrupted your aggregates.

No failure.
No OOM.
No red alerts.

Just wrong numbers in production.

And nobody noticed for 3 days.

🔥 Scenario

You’re a Senior Data Engineer at a global payments platform.

You run a Structured Streaming job that:

• Ingests Kafka transactions (~120K events/sec)
• Deduplicates on transaction_id
• Aggregates 1-hour rolling fraud metrics per merchant_id
• Writes to Delta (exactly-once required)
• SLA: < 2 minutes

After a broker rebalance + network glitch:

• Some metrics doubled
• Some windows missing data
• No job failure
• Checkpoint intact

Business reports fraud spike.

Pipeline “ran successfully.”

📦 Input Dataset (Kafka → Parsed Schema)

transaction_id (string)
merchant_id (string)
user_id (string)
amount (double)
event_time (timestamp)
ingestion_time (timestamp)

🎯 5 Core Questions

1️⃣ Why did aggregates double without failure?
2️⃣ Why didn’t checkpoint protect from duplication?
3️⃣ How does watermark interact with reprocessing?
4️⃣ How do you design idempotent streaming writes?
5️⃣ How do you validate correctness in streaming systems?



1️⃣ Why aggregates doubled?

Kafka rebalance caused offset replay.
Without deduplication → same transaction processed twice.

Checkpoint tracks offsets.
It does NOT guarantee event uniqueness.

2️⃣ Why checkpoint didn’t save you?

Checkpoint ensures:

• Offset progress
• State recovery

It does NOT prevent:
• Duplicate event delivery
• Producer retries
• Rebalanced offset replay

Streaming is at-least-once by default.

3️⃣ Watermark Interaction

Watermark controls state eviction.

If late data re-enters before watermark expires:
It re-updates window.

If watermark too relaxed:
State grows.
Reprocessing amplifies.

4️⃣ Idempotent Write Design

Never rely on append mode in critical metrics.

Use:

• foreachBatch
• Deterministic MERGE
• Window start as unique key

This makes writes safe across retries.

5️⃣ Validation Strategy

Add validation layer:

validation_df = fraud_metrics \
    .groupBy("merchant_id") \
    .agg(sum("txn_count").alias("total_txn"))

validation_df.write.mode("overwrite").save("/mnt/audit/check")

# ================================================
# Day 11 – Rare Streaming Duplication Scenario
# ================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("Day11_Streaming_Idempotent_Design") \
    .config("spark.sql.shuffle.partitions", 800) \
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
    .getOrCreate()

# -----------------------------------------------
# 1️⃣ Read Kafka Stream
# -----------------------------------------------

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_time", TimestampType()),
    StructField("ingestion_time", TimestampType())
])

parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# -----------------------------------------------
# 2️⃣ Deduplicate Safely (Stateful)
# -----------------------------------------------
# Problem:
# If Kafka replays offsets after rebalance,
# duplicates enter stream even with checkpoint intact.

dedup_stream = parsed_stream \
    .withWatermark("event_time", "2 hours") \
    .dropDuplicates(["transaction_id"])

# -----------------------------------------------
# 3️⃣ Rolling Fraud Window Aggregation
# -----------------------------------------------

fraud_metrics = dedup_stream \
    .groupBy(
        window(col("event_time"), "1 hour"),
        col("merchant_id")
    ) \
    .agg(
        count("*").alias("txn_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )

# -----------------------------------------------
# 4️⃣ Idempotent Write Using MERGE
# -----------------------------------------------
# Why?
# Append mode can double-write during retries.
# We need deterministic upsert logic.

def upsert_to_delta(batch_df, batch_id):

    delta_path = "/mnt/delta/fraud_metrics"

    if not DeltaTable.isDeltaTable(spark, delta_path):
        batch_df.write.format("delta") \
            .mode("overwrite") \
            .save(delta_path)
        return

    delta_table = DeltaTable.forPath(spark, delta_path)

    delta_table.alias("target") \
        .merge(
            batch_df.alias("source"),
            """
            target.merchant_id = source.merchant_id
            AND target.window.start = source.window.start
            """
        ) \
        .whenMatchedUpdate(set={
            "txn_count": "source.txn_count",
            "total_amount": "source.total_amount",
            "avg_amount": "source.avg_amount"
        }) \
        .whenNotMatchedInsertAll() \
        .execute()

# -----------------------------------------------
# 5️⃣ Streaming Write with ForeachBatch
# -----------------------------------------------

query = fraud_metrics.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "/mnt/checkpoints/fraud_metrics") \
    .outputMode("update") \
    .start()

query.awaitTermination()
