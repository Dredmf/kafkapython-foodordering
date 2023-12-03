import json

from kafka import KafkaConsumer
from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = 'order_confirmed'


consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC,
    bootstrap_servers = "localhost:29092",
    enable_auto_commit=False,
    group_id = "transaction_01",
    auto_offset_reset='earliest',
    auto_commit_interval_ms=1000
)

producer = KafkaProducer(
    bootstrap_servers = "localhost:29092"
)

print("Gonna start listening")
while True:
    for message in consumer:
        print("Ongoing Transaction ....")
        consumed_message = json.loads(message.value.decode())
        print(consumed_message)
        
        user_id = consumed_message["user_id"]
        total_cost = consumed_message["total"]
        data = {
            "customer_id": user_id,
            "customer_email": f"{user_id}@gmail.com",
            "total_cost": total_cost
        }
        
        print('Succesfull Transaction')
    
        producer.send(
            ORDER_CONFIRMED_KAFKA_TOPIC,
            json.dumps(data).encode("utf-8")
        )
        consumer.commit()
