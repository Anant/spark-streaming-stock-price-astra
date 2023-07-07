import auth
import requests
import time
import pulsar
import json

pulsar_service_url = 'pulsar+ssl://pulsar-gcp-useast1.streaming.datastax.com:6651'
token = auth.pulsar_token
client = pulsar.Client(pulsar_service_url,
                        authentication=pulsar.AuthenticationToken(token))
producer = client.create_producer('persistent://cass-ml-oa/cass-ml/alphavantage')


# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
av_api_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey='+auth.av_api_key
r = requests.get(av_api_url)
data = r.json()

#print(data)
stock_symbol = data['Meta Data']['2. Symbol']
time_series_dict = data['Time Series (5min)']
time_series_dict_keys = time_series_dict.keys()

for key in time_series_dict_keys:
    interval_dict = time_series_dict[key]
    interval_dict['symbol'] = stock_symbol
    interval_dict['time'] = key
    #interval_dict = {k: str(v).encode("utf-8") for k,v in interval_dict.items()}
    print(interval_dict)
    producer.send(json.dumps(interval_dict).encode("utf-8"))
    time.sleep(5)

client.close()