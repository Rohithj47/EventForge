from flask import Flask, request, jsonify, render_template
from confluent_kafka import Producer
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import SimpleStatement
import json
import uuid
from datetime import datetime
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Cassandra config with enhanced retry
def connect_cassandra():
    retries = 10
    delay = 10
    for i in range(retries):
        try:
            logger.info(f"Attempting to connect to Cassandra (attempt {i+1}/{retries})...")
            cluster = Cluster(['cassandra'], port=9042)
            session = cluster.connect('eventforge')
            logger.info("Successfully connected to Cassandra")
            return session
        except NoHostAvailable as e:
            logger.error(f"Cassandra connection failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Unexpected error connecting to Cassandra: {e}")
            raise
    raise Exception("Failed to connect to Cassandra after retries")

session = connect_cassandra()

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
try:
    logger.info("Connecting to Kafka...")
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    logger.info("Successfully connected to Kafka")
except Exception as e:
    logger.error(f"Failed to connect to Kafka: {e}")
    raise

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()}')

@app.route('/events', methods=['POST'])
def ingest_event():
    event = request.get_json()
    if not event or not event.get('user_id') or not event.get('event_type'):
        return jsonify({'error': 'Missing user_id or event_type'}), 400
    
    # Enrich event
    event['event_id'] = str(uuid.uuid4())
    event['timestamp'] = datetime.utcnow().isoformat() + 'Z'
    
    # Publish to Kafka
    try:
        producer.produce(
            topic='raw-events',
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )
        producer.flush()
        return jsonify({'status': 'Event sent', 'event_id': event['event_id']}), 201
    except Exception as e:
        logger.error(f"Error publishing to Kafka: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/')
def dashboard():
    try:
        return render_template('dashboard.html')
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/events', methods=['GET'])
def query_events():
    user_id = request.args.get('user_id')
    event_type = request.args.get('event_type')
    query = "SELECT event_id, event_time, user_id, event_type, metadata FROM events"
    conditions = []
    params = []
    
    if user_id:
        conditions.append("user_id = %s")
        params.append(user_id)
    if event_type:
        conditions.append("event_type = %s")
        params.append(event_type)
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions) + " ALLOW FILTERING" # Note: ALLOW FILTERING can have performance implications
    
    try:
        rows = session.execute(query, params)
        events = [{
            'event_id': str(row.event_id),
            'event_time': row.event_time.isoformat(),
            'user_id': row.user_id,
            'event_type': row.event_type,
            'metadata': dict(row.metadata)
        } for row in rows]
        return jsonify(events)
    except Exception as e:
        logger.error(f"Error querying events: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting Flask app...")
    app.run(host='0.0.0.0', port=5000)