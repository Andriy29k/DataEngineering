from kafka import KafkaConsumer

consumer = KafkaConsumer('travel-data', bootstrap_servers='localhost:29092')

for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")
