from kafka import KafkaConsumer
import json
from collections import defaultdict, deque
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='anomaly_detector'
)

user_transactions = defaultdict(deque)

print("Anomaly detector started...")

for message in consumer:
    event = message.value

    user_id = event['user_id']
    ts = datetime.fromisoformat(event['timestamp'])

    user_transactions[user_id].append(ts)

    while user_transactions[user_id] and (ts - user_transactions[user_id][0]).total_seconds() > 60:
        user_transactions[user_id].popleft()

    if len(user_transactions[user_id]) > 3:
        print(f"ALERT: user {user_id} > 3 transactions in 60s ({len(user_transactions[user_id])})")
        print(event)
