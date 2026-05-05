from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    group_id='enrich-group',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def risk(amount):
    if amount > 3000:
        return "HIGH"
    elif amount > 1000:
        return "MEDIUM"
    return "LOW"

for msg in consumer:
    tx = msg.value
    tx["risk_level"] = risk(tx["amount"])
    print(tx)
