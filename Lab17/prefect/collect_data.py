from kafka import KafkaProducer
import json
import requests

def collect_and_send_data():
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    url = 'https://api.spacexdata.com/v4/launches/past'
    response = requests.get(url)
    data = response.json()

    for entry in data:
        if entry["name"] == "DemoSat":
            mission_data = {
                "mission_name": entry["name"],
                "failures": [{"time": failure["time"], "altitude": failure["altitude"], "reason": failure["reason"]} for failure in entry.get("failures", [])],
                "date_utc": entry["date_utc"],
                "rocket": entry["rocket"]
            }
            producer.send('demo-missions-topic', value=mission_data)
            print(f"Sent: {mission_data}")

    producer.flush()
    producer.close()

if __name__ == "__main__":
    collect_and_send_data()
