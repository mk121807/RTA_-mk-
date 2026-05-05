from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='file-writer'
)

with open("data/transactions_10k.jsonl", "w") as f:
    print("Zapisuję dane do pliku...")

    count = 0
    for message in consumer:
        json.dump(message.value, f)
        f.write("\n")
        count += 1

        if count >= 10000:
            break

print("Zapisano 10k rekordów ")
