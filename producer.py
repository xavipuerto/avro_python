from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import json
import time
import socket

# 🔹 Detectar si estamos en Docker
def get_kafka_broker():
    try:
        socket.gethostbyname("broker")  # Si "broker" se resuelve, estamos en Docker
        return "broker:29092"
    except socket.gaierror:
        return "localhost:9092"

KAFKA_BROKER = get_kafka_broker()
SCHEMA_REGISTRY_URL = "http://schema_registry:8081" if KAFKA_BROKER.startswith("broker") else "http://localhost:8081"

print(f"📌 Conectando a Kafka en: {KAFKA_BROKER}")
print(f"📌 Conectando a Schema Registry en: {SCHEMA_REGISTRY_URL}")

# 🔹 Configuración de Schema Registry
schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
try:
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
except Exception as e:
    print(f"❌ Error conectando con Schema Registry: {e}")
    exit(1)

# 🔹 Esquema Avro para los datos de los sensores
SCHEMA_STR = """
{
    "type": "record",
    "name": "SensorData",
    "fields": [
        {"name": "sensor_id", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "temperatura", "type": "float"},
        {"name": "humedad", "type": "float"},
        {"name": "presion", "type": "float"}
    ]
}
"""

avro_serializer = AvroSerializer(
    schema_registry_client,
    SCHEMA_STR,
    conf={"auto.register.schemas": True}  # 🔹 Registra automáticamente el esquema
)

# 🔹 Configuración del productor de Kafka
producer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "socket.timeout.ms": 5000,  # Evita bloqueos en la conexión
}

try:
    producer = Producer(producer_conf)
except Exception as e:
    print(f"❌ Error conectando con Kafka: {e}")
    exit(1)

# 🔹 Callback de confirmación de entrega
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Error enviando mensaje: {err}")
    else:
        print(f"✅ Mensaje enviado a {msg.topic()} [{msg.partition()}]")

# 🔹 Definir el tema de Kafka
TOPIC = "sensor_data"

# 🔹 Enviar 10 mensajes con datos simulados de sensores
print("🚀 Enviando datos a Kafka...")
try:
    for i in range(10):
        data = {
            "sensor_id": f"sensor_{i}",
            "timestamp": int(time.time()),
            "temperatura": 20.5 + i,
            "humedad": 50.0 + i,
            "presion": 1013.25 + i
        }

        avro_data = avro_serializer(
            data,
            SerializationContext(TOPIC, MessageField.VALUE)
        )

        producer.produce(TOPIC, value=avro_data, callback=delivery_report)
        time.sleep(1)  # Simula una lectura cada segundo

    producer.flush()  # 🔹 Espera a que todos los mensajes sean enviados
    print("🎉 Finalizado envío de datos")

except Exception as e:
    print(f"❌ Error produciendo mensajes: {e}")
