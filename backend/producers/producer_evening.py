from confluent_kafka import Producer
import pandas as pd
import json
import time
import random
import os

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
producer = Producer({
    "bootstrap.servers": bootstrap_servers
})
df = pd.read_parquet("processed/flight_stream.parquet")
print("EVENING producer started", flush=True)
while True:
    row = df.sample(1).iloc[0].to_dict()
    row["CRSDepHour"] = random.randint(17, 23)
    row["source"] = "evening"
    producer.produce("raw_flights", json.dumps(row))
    producer.poll(0)
    producer.flush(0)
    time.sleep(random.uniform(0.05, 0.1))