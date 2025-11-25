import datetime
from kafka import KafkaConsumer
import json
import pymongo

# Kafka configuration
bootstrap_servers = '10.90.43.165:9092'
topic_name = 'esp32_raw_data'

# MongoDB configuration
#conex = pymongo.MongoClient(host=[MongoDB + ':27017'])
conex = pymongo.MongoClient(host=['127.0.0.1:27017'], username='admin', password='parda99*')
db = conex.esp32_data

# Create Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    request_timeout_ms=60000
)

# Reader process (into loop)
try:
    for message in consumer:
        # Extract data from message
        deviceID = message.value['topic'].split(';')[0]
        ambientLight = message.value['payload'].split(';')[1]
        temperature = message.value['payload'].split(';')[2]

        # Show data in the console
        print(f"Dispositivo: {deviceID} | Luz Ambiental: {ambientLight} | Temperatura: {temperature} ")
        print("-" * 85)

        try:
            # Insert data into MongoDB
            db.sensores.insert_one({
                'device_id': deviceID,
                'light_level': int(ambientLight),
                'temperature_level': float(temperature),
                'status': 'received',
                'timestamp': datetime.datetime.now()
            })
        except pymongo.errors.PyMongoError as e:
            print(f"Error al guardar en MongoDB: {e}")

except KeyboardInterrupt:
    print("Consumidor detenido por el usuario.")
