Scenario - 

You are a Senior Data Engineer (L5/L6) at a global payments company.

You built a real-time fraud detection feature pipeline.

Scale:

• 180K events/sec peak
• 12 TB/day ingestion
• 14-day rolling window
• Aggregation by user_id
• Exactly-once requirement
• SLA: < 60 seconds freshness

After 4 days in production:

• Executors crash with OOM
• Checkpoint directory grows rapidly
• Restart takes 45 minutes
• Latency increases gradually each day
• No code change


🎯 Core Questions

1 - What is Spark storing in the state store?
2 - Why does state grow daily?
3 - What does watermark actually do?
4 - Why is restart slow?
5 - Why does checkpoint explode?
6 - How do you make state bounded?
7 - How do you scale 5× safely?
8 - How do you ensure exactly-once semantics?

🧠 Answers
1️⃣ What Is Stored in State Store?

Spark stores:

• Key (user_id + window)
• Aggregation buffers
• Offset metadata

It does NOT store raw events.
It stores intermediate aggregation state.

If 50M active users × 14-day window
State size explodes.

2️⃣ Why State Grows

Because:

• Late data extends window
• Watermark too relaxed
• Sliding windows overlap
• High cardinality keys
• No aggressive eviction

State grows linearly unless bounded.

3️⃣ What Watermark Actually Does

Watermark defines:

“How late can data arrive before eviction?”

Without watermark → state never evicts.
Incorrect watermark → data loss risk.

4️⃣ Why Restart Is Slow

On restart:

• State store reload
• Aggregation buffer reconstruction
• Checkpoint metadata replay

Large checkpoint = long recovery time.

5️⃣ Why Checkpoint Explodes

• Every micro-batch writes metadata
• State updates persist incrementally
• Many small files accumulate
• No compaction strategy

6️⃣ How To Bound State

• Aggressive watermark
• Reduce window size
• Pre-aggregate before window
• Avoid unnecessary high-cardinality keys
• Use session window if applicable

7️⃣ Exactly-Once Guarantee

Spark ensures:

• Offset tracking
• Atomic commits
• Idempotent sink writes

Delta Lake strongly recommended.



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window

spark = SparkSession.builder \
    .appName("Streaming_State_Debug") \
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
    .getOrCreate()

# Read streaming data
stream_df = spark.readStream \
    .format("delta") \
    .load("/mnt/events")

# Apply watermark and window aggregation
aggregated_stream = (
    stream_df
    .withWatermark("event_timestamp", "2 days")
    .groupBy(
        col("user_id"),
        window(col("event_timestamp"), "14 days", "1 day")
    )
    .count()
)

# Tune shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 2000)

# Control state cleanup interval
spark.conf.set("spark.sql.streaming.stateStore.maintenanceInterval", "60s")

# Exactly-once sink using Delta
query = aggregated_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/fraud_pipeline") \
    .outputMode("append") \
    .start("/mnt/output/fraud_features")

query.awaitTermination()
