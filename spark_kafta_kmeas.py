from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

spark = (
    SparkSession.builder
    .appName("Task3-Streaming-KMeans")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("timestamp_utc", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("store", StringType(), True),
])

# 1) Read from Kafka (real-time stream)
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "transactions_topic")
    .option("startingOffsets", "latest")
    .load()
)

# 2) Kafka value is bytes -> cast to string -> parse JSON
json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str")
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("t")).select("t.*")

# 3) Build features for clustering (keep it simple: amount only)
feature_df = parsed_df.where(col("amount").isNotNull())

assembler = VectorAssembler(inputCols=["amount"], outputCol="features")
vector_df = assembler.transform(feature_df).select("transaction_id", "amount", "features")

def train_and_show(batch_df, batch_id: int):
    if batch_df.count() == 0:
        return

    kmeans = KMeans(k=3, seed=42, featuresCol="features", predictionCol="cluster")
    model = kmeans.fit(batch_df)

    clustered = model.transform(batch_df)
    print(f"\n=== Batch {batch_id} ===")
    clustered.groupBy("cluster").count().show(truncate=False)
    clustered.select("transaction_id", "amount", "cluster").show(10, truncate=False)

# 4) Train per micro-batch (streaming)
query = (
    vector_df.writeStream
    .foreachBatch(train_and_show)
    .outputMode("update")
    .start()
)

query.awaitTermination()
