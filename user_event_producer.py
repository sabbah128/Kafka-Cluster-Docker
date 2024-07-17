from datetime import datetime
import json
from kafka import KafkaProducer
import random
import time
import uuid


TOPIC_NAME = 'eventsTopic'
EVENT_TYPE_LIST = ['buy', 'sell', 'click', 'hover', 'idle_5']

producer = KafkaProducer(
   bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
   value_serializer=lambda msg: json.dumps(msg).encode('utf-8'), 
   key_serializer=str.encode)


def produce_event():
    return {
        'event_id': str(uuid.uuid4()),
        'event_datetime': datetime.now().strftime('%Y-%m-%d-%H-%M-%S'),
        'event_type': random.choice(EVENT_TYPE_LIST)
    }

def send_events():
    while(True):
        data = produce_event()
        producer.send(TOPIC_NAME, value=data, key=data['event_id'])
        print(f"Event Created : {data['event_id']}")
        time.sleep(3)


if __name__ == '__main__':
    send_events()