import threading
from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

def sensor_instance(sensor_id):
    # Create Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Topic name
    my_name = "iuliia"
    topic_name = f'{my_name}_building_sensors'

    # Simulate sensor data and send to Kafka topic
    try:
        for i in range(10):
            data = {
                "sensor_id": sensor_id,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "temperature": round(random.uniform(25, 45), 2),  # Random temperature
                "humidity": round(random.uniform(15, 85), 2)  # Random humidity
            }
            producer.send(topic_name, key=str(uuid.uuid4()), value=data)
            producer.flush()
            print(f"Message {i + 1} sent to topic '{topic_name}': {data}")
            time.sleep(2)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.close()

threads = []
for _ in range(2):
    sensor_id = random.randint(1000, 9999)
    thread = threading.Thread(target=sensor_instance, args=(sensor_id,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()
