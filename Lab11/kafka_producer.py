import random
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta

producer = KafkaProducer(bootstrap_servers='localhost:29092')

destinations = ["New York", "Paris", "Tokyo", "London", "Berlin"]
transport_types = ["Airplane", "Train", "Bus", "Ship"]

def generate_departure_time():
    departure_time = datetime.now() + timedelta(days=random.randint(1, 30), hours=random.randint(1, 24))
    return departure_time.strftime('%Y-%m-%d %H:%M:%S')

for i in range(120000):  
    passenger_id = random.randint(1000, 9999)
    destination = random.choice(destinations)
    transport_type = random.choice(transport_types)
    departure_time = generate_departure_time()

    message = {
        'passenger_id': passenger_id,
        'destination': destination,
        'transport_type': transport_type,
        'departure_time': departure_time
    }

    producer.send('travel-data', value=str(message).encode('utf-8'))
    print(f"Sent: {message}")

    #time.sleep(1)

producer.flush()
producer.close()
