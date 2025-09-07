from confluent_kafka import Consumer, KafkaError
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import json
from datetime import datetime

# Kafka config
consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'eventforge-consumer',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['raw-events'])

# Cassandra config
cluster = Cluster(['cassandra'])
session = cluster.connect('eventforge')

insert_query = """
    INSERT INTO events (event_id, event_time, user_id, event_type, metadata)
    VALUES (?, ?, ?, ?, ?)
"""
prepared_stmt = session.prepare(insert_query)

def process_event(event):
    data = json.loads(event.value().decode('utf-8'))
    event_time = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
    session.execute(prepared_stmt, (
        uuid.UUID(data['event_id']),
        event_time,
        data['user_id'],
        data['event_type'],
        data.get('metadata', {})
    ))

def main():
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f'Error: {msg.error()}')
                break
        process_event(msg)
    consumer.close()

if __name__ == '__main__':
    main()