import findspark
import auth

findspark.init('/workspace/astra-spark-streaming/spark-3.4.1-bin-hadoop3')

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType




sc = SparkContext("local[2]", "SparkPulsarStreams")
spark = SparkSession(sc)

def writeStocktoAstra(df, epochId):
    df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="stock_data", keyspace="spark_streaming").save()

def replaceColumnNames(aStr):
    return aStr.replace("1. open","open").replace("2. high","high").replace("3. low","low").replace("4. close","close").replace("5. volume","volume")

replaceColumnNamesUDF = udf(lambda x:replaceColumnNames(x),StringType()) 

df = spark \
    .readStream.format("pulsar") \
    .option("service.url", "pulsar+ssl://pulsar-gcp-useast1.streaming.datastax.com:6651") \
    .option("admin.url", "https://pulsar-gcp-useast1.api.streaming.datastax.com") \
    .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationToken") \
    .option("pulsar.client.authParams","token:"+auth.pulsar_token) \
    .option("topic", "persistent://cass-ml-oa/cass-ml/alphavantage"). \
    load()

df.printSchema()

schema = MapType(StringType(),StringType())

newQuery =  df \
            .selectExpr("CAST(value AS STRING)") \
            .withColumn("value", replaceColumnNamesUDF(col("value"))) \
            .withColumn("value", F.from_json("value", schema)) \
            .select("value.open","value.high","value.low","value.close","value.volume","value.symbol","value.time") \
            .withColumn("open", col("open").cast(FloatType())) \
            .withColumn("high", col("high").cast(FloatType())) \
            .withColumn("low", col("low").cast(FloatType())) \
            .withColumn("close", col("close").cast(FloatType())) \
            .withColumn("volume", col("volume").cast(IntegerType())) \
            .withColumn("time", col("time").cast(TimestampType())) \
            .writeStream.foreachBatch(writeStocktoAstra).outputMode("update").start()

newQuery.awaitTermination()
newQuery.stop()