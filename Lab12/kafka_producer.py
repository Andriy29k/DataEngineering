from kafka import KafkaProducer
from time import sleep
import json
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(1000):
    data = {
        'campaign_id': f'campaign_{i % 5}', 
        'opened_emails': i * 3,             
        'clicked_links': i * 2,             
        'timestamp': datetime.now().isoformat()  
    }
    producer.send('email-campaigns', value=data)
    print(f"Sent: {data}")
    sleep(0.5)

producer.flush()
producer.close()
