from pyspark.sql.functions import *
from delta.tables import DeltaTable

updates = spark.read.parquet("/data/product_updates")

dim_table = DeltaTable.forPath(spark,"/delta/product_dim")

dim_df = dim_table.toDF()

updates = updates.withColumn(
"hash",
sha2(concat_ws("||",col("product_name"),col("category"),col("price"),col("status")),256)
)

dim_current = dim_df.filter(col("is_current")==True)

dim_current = dim_current.withColumn(
"hash",
sha2(concat_ws("||",col("product_name"),col("category"),col("price"),col("status")),256)
)

changed = updates.alias("u").join(
dim_current.alias("d"),
"product_id"
).filter(col("u.hash") != col("d.hash"))

dim_table.alias("t").merge(
changed.alias("s"),
"t.product_id = s.product_id AND t.is_current = true"
).whenMatchedUpdate(set={
"effective_to":"s.updated_at",
"is_current":"false"
}).whenNotMatchedInsert(values={
"product_id":"s.product_id",
"product_name":"s.product_name",
"category":"s.category",
"price":"s.price",
"status":"s.status",
"effective_from":"s.updated_at",
"effective_to":"null",
"is_current":"true"
}).execute()
