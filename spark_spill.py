Input datasets : transactions(transaction_id string,user_id string,amount double,event_time timestamp,region string) 

users(user_id string,risk_score double,user_segment string,signup_date date)

Questions
1 - Why does runtime increase even without failures?
2 - What is Spark spilling internally?
3 - Why do rolling windows increase memory usage?
4 - How can we reduce spill architecturally?
5 - How do we optimize this pipeline correctly?

Answers

1️⃣ Runtime increases because Spark does not fail when execution memory is insufficient. It spills intermediate sort buffers, aggregation maps, and window state to disk. Disk I/O + repeated merge passes + GC pressure increase runtime without crashing executors.

2️⃣ Spark spills shuffle partitions, hash aggregation maps, sort buffers (ExternalSorter), and window state structures. When execution memory crosses threshold, Spark writes partial results to disk and merges later.

3️⃣ Rolling windows partitioned by user_id require maintaining ordered state per key. High cardinality users + large frame (30 days) = buffered rows per partition. Memory accumulates before spill triggers.

4️⃣ Architectural reduction means reducing data size before windowing. Instead of windowing raw transaction rows, pre-aggregate daily metrics first. Reduce shuffle size, state size, and sort pressure.

5️⃣ Correct optimization combines logical reduction + join strategy + partition tuning + memory efficiency + window scope reduction. Scaling cluster alone treats symptom, not cause.



from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,count,to_date,broadcast
from pyspark.sql.window import Window

spark=SparkSession.builder.appName("Day7_Memory_Spill").config("spark.sql.adaptive.enabled","true").getOrCreate()

transactions=spark.read.parquet("path/to/transactions")
users=spark.read.parquet("path/to/users")

# Optimize join (avoid shuffle if possible)
joined=transactions.join(broadcast(users),"user_id")

# Reduce dataset early
daily_txn=joined.groupBy("user_id",to_date("event_time").alias("event_date")).agg(
    sum("amount").alias("daily_amount"),
    count("*").alias("daily_count")
)

# Rolling 30-day window on reduced dataset
window_spec=Window.partitionBy("user_id").orderBy("event_date").rowsBetween(-29,0)

final_df=daily_txn.withColumn(
    "total_amount_30d",sum("daily_amount").over(window_spec)
).withColumn(
    "transaction_count_30d",sum("daily_count").over(window_spec)
)

# Tune partitions & memory
spark.conf.set("spark.sql.shuffle.partitions",3000)
spark.conf.set("spark.memory.fraction",0.6)
spark.conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

final_df.write.mode("overwrite").partitionBy("event_date").parquet("path/to/output")
