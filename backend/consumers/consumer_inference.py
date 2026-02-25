from confluent_kafka import Consumer, Producer
import torch
import torch.nn as nn
import joblib
import json
import numpy as np
import os

ohe = joblib.load("model/ohe.joblib")
scaler = joblib.load("model/scaler.joblib")

class FlightDelayModel(nn.Module):
    def __init__(self, input_dim):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, 1),
            nn.Sigmoid()
        )
    def forward(self, x):
        return self.net(x)

cat_dim = ohe.transform([["1","10000","10000"]]).shape[1]
input_dim = cat_dim + 2

model = FlightDelayModel(input_dim)
model.load_state_dict(torch.load("model/flight_delay_model.pt", map_location="cpu"))
model.eval()
print("Model loaded")
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
group_id = os.getenv("KAFKA_GROUP_ID", "inference_group")
consumer = Consumer({
    "bootstrap.servers": bootstrap_servers,
    "group.id": group_id,
    "auto.offset.reset": "earliest"
})
producer = Producer({
    "bootstrap.servers": bootstrap_servers
})
consumer.subscribe(["raw_flights"])
print("Inference consumer started")
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue
    data = json.loads(msg.value().decode())
    X_cat = ohe.transform([[data["DayOfWeek"],
                            data["OriginAirportID"],
                            data["DestAirportID"]]])
    X_num = scaler.transform([[data["Distance"],
                               data["CRSDepHour"]]])
    X = np.hstack([X_cat, X_num])
    with torch.no_grad():
        prob = model(torch.FloatTensor(X)).item()
    result = {
        "probability": prob,
        "true_label": data["DepDel15"]
    }
    producer.produce("predictions", json.dumps(result))
    producer.poll(0)
    producer.flush(0)