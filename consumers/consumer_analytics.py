from confluent_kafka import Consumer
import json
from collections import deque
import statistics
import os

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
group_id = os.getenv("KAFKA_GROUP_ID", "analytics_group")
consumer = Consumer({
    "bootstrap.servers": bootstrap_servers,
    "group.id": group_id,
    "auto.offset.reset": "earliest"
})
consumer.subscribe(["predictions"])
window = deque(maxlen=200)
print("Analytics consumer started")
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue
    data = json.loads(msg.value().decode())
    window.append(data)
    probs = [x["probability"] for x in window]
    preds = [1 if p > 0.5 else 0 for p in probs]
    trues = [x["true_label"] for x in window]
    accuracy = sum(p == t for p, t in zip(preds, trues)) / len(trues)
    avg_prob = statistics.mean(probs)
    print("-----")
    print("Window:", len(window))
    print("Accuracy:", round(accuracy, 3))
    print("Avg probability:", round(avg_prob, 3))
