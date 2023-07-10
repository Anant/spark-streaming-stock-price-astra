# astra-spark-streaming
## Quickstart in Gitpod

You can either follow the directions below or [open this in gitpod](https://gitpod.io/#https://github.com/Anant/pytorch-astra-demo).

[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/Anant/pytorch-astra-demo)

# Setting up AstraDB
1. Go to astra.datastax.com and sign up for a free account.
2. Create a database using the add database button.
3. Go to the CQL Console and create the required tables.
```
CREATE TABLE spark_streaming.stock_data ( symbol text, time timestamp, open decimal, high decimal, low decimal, close decimal, volume int, PRIMARY KEY ((symbol), time));
```
4. Download a secure connect bundle.
5. Generate a Database Administrator token.
## Connecting to AstraDB in the code environment
6. Load the secure connect bundle into the environment.
7. Input the bundle filepath, the client id, and the token into auth.py .

# Setting up Astra Streaming
8. Sign on to astra.datastax.com
9. Hit the streaming button on the left side of the screen
10. Create a tenant
11. Go to the Namespace and Topic Field and create a namespace
12. Add a topic to your name space
13. Copy out your Broker Service URL, Web Service URL from the Pular section under Connect
14. Paste the Broker Service URL as pulsar_service_url on line 7 of create_raw_stream.py and as service.url on line 30 of pyspark_stream_read.py
15. Paste the Web Service URL as admin.url on line 31 of pyspark_stream_read.py
16. Go to the setting tab and copy out your token from the token manager
17. Add your pulsar token to auth.py

## Other set up for the code environment
18. Sign up for an alphavantage api key [here.](https://www.alphavantage.co/)
19. Add your alphavantage key to auth.py
20. Install python requirements using pip3 install -r requirements.txt
21. Start the Spark Master by running ./spark-3.4.1-bin-hadoop3/sbin/start-master.sh
22. Open port 8080 and copy out the Spark Master address
23. Start the Spark Worker by running ./spark-3.4.1-bin-hadoop3/sbin/start-worker.sh <Spark Master Address>

# Running the example code
24. In one terminal run create_raw_stream.py. If you want to change the API call, edit it on line 15 first.
25. In another tab, run the spark submit command for pyspark_stream_read.py
```
./spark-3.4.1-bin-hadoop3/bin/spark-submit \
--master <Spark Master Address> \
--packages io.streamnative.connectors:pulsar-spark-connector_2.12:3.1.1.3,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
--conf spark.files=<bundle filepath> \
--conf spark.cassandra.connection.config.cloud.path=<bundle file name> \
--conf spark.cassandra.auth.username=<astra client id> \
--conf spark.cassandra.auth.password=<astra client secret> \
--conf spark.dse.continuousPagingEnabled=false \
<path to pyspark_stream_read.py>
```