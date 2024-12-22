from kafka import KafkaProducer
import json
import requests

# API SpaceX
def collect_and_send_data():
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    url = 'https://api.spacexdata.com/v4/launches'
    response = requests.get(url)
    data = response.json()
    
    failed_launches = [
        {
            "mission_name": launch.get("name"),
            "failures": launch.get("failures", []),
            "date_utc": launch.get("date_utc"),
            "rocket": launch.get("rocket"),
        }
        for launch in data
        if not launch.get("success") and "DemoSat" in launch.get("name", "")
    ]
    
    for entry in failed_launches:
        print(f"Sending: {entry}")
        producer.send('spacex-failures-topic', value=entry)
    
    producer.flush()
    producer.close()

if __name__ == "__main__":
    collect_and_send_data()
