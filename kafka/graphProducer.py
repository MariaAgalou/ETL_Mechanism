from kafka import KafkaProducer
import json, requests

# Kafka configuration
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# Fetch data from Flask API and publish it to Kafka topic
def produce_messages(username: str):
    
    # URL for Flask API
    url = f'http://127.0.0.1:5001/users?name={username}'

    try:
        # Make request
        response = requests.get(url)
        data = response.json()

        if 'error' in data:
            print(f"Error while trying to fetch data: {data['error']}")
            return

        # Publish data to Kafka topic
        for user in data:
            producer.send('users-topic', user)
        
        producer.flush()
        print("Data sent successfully to Kafka topic 'users-topic'")
    
    # Error while trying to fetch data from Flask / HTTP request failed
    except requests.RequestException as e:
        print(f'Could not fetch data from Flask API due to: {e}')
    
    # Error while trying to publish messages to Kafka topic / Kafka message publishing failed
    except KafkaProducer.KafkaError as c:
        print(f'Could not publish messages to Kafka topic due to: {c}')


if __name__ == '__main__':
    produce_messages('Maria')

    # username = input("Enter username: ")
    # produce_messages(username)
