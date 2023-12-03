import json
import time

from kafka import KafkaProducer, producer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 125

producer = KafkaProducer(bootstrap_servers = "localhost:29092", acks = 'all')

print("Going to be generating order after 10 seconds")
print("Will generate one unique order every 10 seconds")

for i in range(1, ORDER_LIMIT):
    data = {
        "order_id": i,
        "user_id": f"tom_{i}",
        "total": i * 2 ,
        "items": "burger, sandwich"
    }
    
    producer.send(
        ORDER_KAFKA_TOPIC,
        json.dumps(data).encode("utf-8")
    )
    print(json.dumps(data).encode("utf-8"))
    print(f"Done sending ..... {i}")
    time.sleep(3)