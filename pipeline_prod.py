💡 Day 10 – PySpark Scenario-Based Interview Question  

Can your Spark job recompute 5TB of revenue twice and return the exact same number?

If not, you’re not production-ready.

Scenario:

You are a Senior Data Engineer at a global marketplace.

You must build a daily revenue reconciliation pipeline.

Scale:

• 5TB transactions per day  
• 200M users  
• Multi-currency  
• Partial refunds  
• Late events up to 3 days  
• Exactly-once requirement  
• SLA: 60 minutes  

You receive data from 3 independent systems:

1️⃣ orders  
2️⃣ payments  
3️⃣ refunds  

Each system can arrive late or out of order.

📥 Input Datasets:

orders(order_id string, user_id string, product_id string, amount double, currency string, order_time timestamp, ingestion_time timestamp)  
payments(payment_id string, order_id string, paid_amount double, payment_status string, payment_time timestamp, ingestion_time timestamp)  
refunds(refund_id string, order_id string, refund_amount double, refund_time timestamp, ingestion_time timestamp)

🎯 Requirement  

Produce:

daily_revenue(order_date date, total_orders bigint, gross_revenue double, refund_amount double, net_revenue double, mismatch_count bigint)

Rules:

• Only successfully paid orders count  
• Partial refunds reduce net revenue  
• Detect payment mismatches  
• Idempotent reprocessing  
• Handle late data  
• Avoid double counting  

🎯 Questions  

1. How do you deduplicate late-arriving data safely?  
2. How do you prevent double counting during retries?  
3. How do you compute net revenue correctly with partial refunds?  
4. How do you detect reconciliation mismatches?  
5. How do you design incremental processing?  
6. How do you normalize multi-currency aggregation?  
7. How do you optimize large joins at 5TB scale?  
8. How do you guarantee exactly-once semantics?  
9. How do you make the pipeline safely re-runnable?  
10. How do you validate financial correctness before publishing?  


# ==========================================================
# Day 10 – Large Scale Revenue Reconciliation Pipeline
# ==========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Day10_Revenue_Reconciliation") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", 5000) \
    .getOrCreate()

# ----------------------------------------------------------
# 1️⃣ Watermark-Based Incremental Load
# ----------------------------------------------------------

watermark_ts = "2026-02-01 00:00:00"

orders = spark.read.parquet("path/orders") \
    .filter(col("ingestion_time") > lit(watermark_ts))

payments = spark.read.parquet("path/payments") \
    .filter(col("ingestion_time") > lit(watermark_ts))

refunds = spark.read.parquet("path/refunds") \
    .filter(col("ingestion_time") > lit(watermark_ts))

# ----------------------------------------------------------
# 2️⃣ Deduplicate Using Latest Ingestion
# ----------------------------------------------------------

order_window = Window.partitionBy("order_id") \
    .orderBy(col("ingestion_time").desc())

orders_dedup = orders.withColumn(
    "rn", row_number().over(order_window)
).filter(col("rn") == 1).drop("rn")

payment_window = Window.partitionBy("order_id") \
    .orderBy(col("ingestion_time").desc())

payments_dedup = payments.withColumn(
    "rn", row_number().over(payment_window)
).filter(col("rn") == 1).drop("rn")

# ----------------------------------------------------------
# 3️⃣ Filter Successful Payments
# ----------------------------------------------------------

payments_success = payments_dedup.filter(
    col("payment_status") == "SUCCESS"
)

# ----------------------------------------------------------
# 4️⃣ Aggregate Refunds Per Order
# ----------------------------------------------------------

refunds_agg = refunds.groupBy("order_id") \
    .agg(sum("refund_amount").alias("total_refund"))

# ----------------------------------------------------------
# 5️⃣ Join Orders + Payments
# ----------------------------------------------------------

orders_paid = orders_dedup.join(
    payments_success,
    "order_id",
    "inner"
)

# ----------------------------------------------------------
# 6️⃣ Join Refunds
# ----------------------------------------------------------

orders_full = orders_paid.join(
    refunds_agg,
    "order_id",
    "left"
).fillna({"total_refund": 0})

# ----------------------------------------------------------
# 7️⃣ Currency Normalization (Assume Pre-Loaded FX Table)
# ----------------------------------------------------------

fx_rates = spark.read.parquet("path/fx_rates")
orders_fx = orders_full.join(
    broadcast(fx_rates),
    "currency"
)

orders_normalized = orders_fx.withColumn(
    "amount_usd",
    col("amount") * col("fx_rate")
).withColumn(
    "refund_usd",
    col("total_refund") * col("fx_rate")
)

# ----------------------------------------------------------
# 8️⃣ Compute Revenue Metrics
# ----------------------------------------------------------

orders_final = orders_normalized.withColumn(
    "order_date",
    to_date("order_time")
)

daily_revenue = orders_final.groupBy("order_date") \
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("amount_usd").alias("gross_revenue"),
        sum("refund_usd").alias("refund_amount")
    ).withColumn(
        "net_revenue",
        col("gross_revenue") - col("refund_amount")
    )

# ----------------------------------------------------------
# 9️⃣ Detect Mismatches
# ----------------------------------------------------------

mismatch_df = orders_final.filter(
    abs(col("paid_amount") - col("amount")) > 0.01
)

mismatch_daily = mismatch_df.groupBy("order_date") \
    .agg(count("*").alias("mismatch_count"))

daily_revenue_final = daily_revenue.join(
    mismatch_daily,
    "order_date",
    "left"
).fillna({"mismatch_count": 0})

# ----------------------------------------------------------
# 🔟 Write Idempotent Output
# ----------------------------------------------------------

daily_revenue_final.repartition("order_date") \
    .write \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .parquet("path/output/daily_revenue")
