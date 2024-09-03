from kafka import KafkaProducer
import json, requests

# Kafka configuration
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# Fetch data from Flask API and publish it to Kafka topic
def produce_messages(start_date, end_date):

    # URL for Flask API
    url = f'http://127.0.0.1:5001/bands_by_dates?start_date={start_date}&end_date={end_date}'

    try:
        # Make request
        response = requests.get(url)
        data = response.json()

        if 'error' in data:
            print(f"Error while trying to fetch data: {data['error']}")
            return

        # Publish data to Kafka topic
        for band in data:
            producer.send('bands-topic', band)
        
        producer.flush()
        print("Data sent successfully to Kafka topic 'bands-topic'")
    
    # Error while trying to fetch data from Flask / HTTP request failed
    except requests.RequestException as e:
        print(f'Could not fetch data from Flask API due to: {e}')
    
    # Error while trying to publish messages to Kafka topic / Kafka message publishing failed
    except KafkaProducer.KafkaError as c:
        print(f'Could not publish messages to Kafka topic due to: {c}')


if __name__ == '__main__':

    produce_messages('2000-01-01T00:00:00', '2010-12-31T23:59:59')

    # start_date = input("Enter the start date (in ISO format, e.g., '2000-01-01T00:00:00'): ")
    # end_date = input("Enter the end date (in ISO format, e.g., '2010-12-31T23:59:59'): ")

    # produce_messages(start_date, end_date)
