import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import json

# MQTT configuration
MQTT_BROKER_HOST = "10.90.43.165"
MQTT_BROKER_PORT = 1883
MQTT_TOPIC = "esp32/data"

# Kafka configuration
KAFKA_BROKER_HOST = "10.90.43.165:9092"  # Dirección de tu broker Kafka
KAFKA_TOPIC = "esp32_raw_data"  # El tópico donde quieres enviar los datos

# Run callback when client to connect with MQTT Broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conectado al broker MQTT")
        client.subscribe(MQTT_TOPIC)
        print(f"Suscrito al tópico: {MQTT_TOPIC}")
    else:
        print(f"Error al conectar al broker MQTT. Código de retorno: {rc}")

# Run callback when receives a MQTT message
def on_message(client, userdata, msg):
    print(f"Mensaje recibido en '{msg.topic}': {msg.payload.decode()}")

    # Try to send message to Kafka broker
    try:
        # Sending message a plain text (you can apply JSON format if needed)
        message_to_send = {"topic": msg.topic, "payload": msg.payload.decode()}
        kafka_producer.send(KAFKA_TOPIC, value=message_to_send)
        print(f"Mensaje enviado a Kafka: {message_to_send}")

    except Exception as e:
        print(f"Error al enviar el mensaje a Kafka: {e}")

if __name__ == "__main__":
    try:
        # 1. Create a Kafka producer instance
        kafka_producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER_HOST],
            api_version=(0, 10),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serializa el valor a bytes
        )

        # 2. MQTT client configuration
        mqtt_client = mqtt.Client()
        mqtt_client.on_connect = on_connect
        mqtt_client.on_message = on_message
        mqtt_client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)

        # 3. Init the MQTT loop
        mqtt_client.loop_forever()

    except Exception as e:
        print(f"Error principal: {e}")
