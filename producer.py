import json
from kafka import KafkaProducer
from time import sleep

bootstrap_servers = ['localhost:9092']
topic1 = 'fdm'

producer1 = KafkaProducer(bootstrap_servers=bootstrap_servers,
                          value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Read data from the preprocessed.json file
with open('/home/hdoop/kafka/Assignment2/preprocessed_data.json', 'r') as f:
    data = f.readlines()

# Iterate over each line in the file
for line in data:
    # Load JSON object from each line
    obj = json.loads(line.strip())
    # Send the JSON object to Kafka
    producer1.send(topic1, value=obj)
    print('Data sent to Kafka topic:', obj)
    sleep(2)  
