import findspark

findspark.init('/workspace/astra-spark-streaming/spark-3.4.1-bin-hadoop3')

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local[2]", "SparkPulsarStreams")
spark = SparkSession(sc)

df = spark \
    .readStream.format("pulsar") \
    .option("service.url", "pulsar+ssl://pulsar-gcp-useast1.streaming.datastax.com:6651") \
    .option("admin.url", "https://pulsar-gcp-useast1.api.streaming.datastax.com") \
    .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationToken") \
    .option("pulsar.client.authParams","token:") \
    .option("topic", "persistent://cass-ml-oa/cass-ml/alphavantage"). \
    load()

df.printSchema()

pQuery = df.selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)").writeStream.format("console").option("truncate", "false").start()

pQuery.explain()
pQuery.awaitTermination()
pQuery.stop()