from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {"count": 0, "sum": 0, "min": float("inf"), "max": 0})

msg_count = 0

for msg in consumer:
    tx = msg.value
    c = tx["category"]
    a = tx["amount"]

    stats[c]["count"] += 1
    stats[c]["sum"] += a
    stats[c]["min"] = min(stats[c]["min"], a)
    stats[c]["max"] = max(stats[c]["max"], a)

    msg_count += 1

    if msg_count % 10 == 0:
        print("\n=== CATEGORY STATS ===")
        for k, v in stats.items():
            print(k, v)
