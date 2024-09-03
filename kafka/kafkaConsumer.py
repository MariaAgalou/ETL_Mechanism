from kafka import KafkaConsumer
import json, requests

# Kafka configuration
consumer = KafkaConsumer('bands-topic', 'users-topic', bootstrap_servers='localhost:9092',
                         group_id='integration-group', auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Consume data from Kafka topics, integrate them and save them to MySQL Database
def consume_messages(msg):

    # URL for Flask API
    url = f'http://127.0.0.1:5001/insert_to_mysql'

    data = {
        "topic" : msg.topic,
        "value" : msg.value
    }

    try:
        response = requests.post(url=url, json=data)
        # Check response status
        if response.status_code == 201:
            print("Data successfully inserted into MySQL.")
        else:
            print(f"Error: {response.json()}")
    except requests.RequestException as e:
        print(f"Failed to send data to Flask endpoint: {e}")



if __name__ == '__main__':
    # Consume messages from both Kafka topics
    for message in consumer:
        consume_messages(message)
        print("Message consumed")
