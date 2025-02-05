from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import psycopg2
import socket
import json
import time
import sys

# 🔹 Configuración de conexión a PostgreSQL
DB_CONFIG = {
    "dbname": "sensores_db",
    "user": "sensor_user",
    "password": "sensor_password",
    "host": "localhost",
    "port": "5432"
}

# 🔹 Función para insertar datos en PostgreSQL
def insert_data(cursor, sensor_id, timestamp, temperatura, humedad, presion):
    cursor.execute(
        """
        INSERT INTO sensores (sensor_id, timestamp, temperatura, humedad, presion)
        VALUES (%s, to_timestamp(%s), %s, %s, %s)
        """,
        (sensor_id, timestamp, temperatura, humedad, presion)
    )

# 🔹 Detección automática del entorno (Docker o máquina local)
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

# 🔹 Conexión a Schema Registry
schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
try:
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
except Exception as e:
    print(f"❌ Error conectando con Schema Registry: {e}")
    exit(1)

# 🔹 Esquema Avro (debe coincidir con el `producer.py`)
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

avro_deserializer = AvroDeserializer(
    schema_registry_client,
    SCHEMA_STR
)

# 🔹 Configuración del consumidor de Kafka
consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "sensor_consumers",
    "auto.offset.reset": "earliest",  # Leer desde el inicio si no hay offset guardado
}

try:
    consumer = Consumer(consumer_conf)
except Exception as e:
    print(f"❌ Error conectando con Kafka: {e}")
    exit(1)

# 🔹 Conexión a PostgreSQL (una sola vez, en lugar de por mensaje)
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("✅ Conectado a PostgreSQL correctamente.")
except Exception as e:
    print(f"❌ Error conectando a PostgreSQL: {e}")
    exit(1)

# 🔹 Suscribirse al tópico de Kafka
TOPIC = "sensor_data"
consumer.subscribe([TOPIC])
print(f"🚀 Esperando mensajes en el tópico: {TOPIC}...\n")

# 🔹 Procesar mensajes de Kafka en un bucle infinito
try:
    while True:
        msg = consumer.poll(1.0)  # Esperar 1 segundo por un mensaje

        if msg is None:
            continue  # No hay mensajes disponibles

        if msg.error():
            print(f"❌ Error en el mensaje: {msg.error()}")
            continue

        # 🔹 Deserializar mensaje Avro
        try:
            compressed_size = len(msg.value())
            print(f"📦 Tamaño del mensaje comprimido: {compressed_size} bytes")

            sensor_data = avro_deserializer(msg.value(), SerializationContext(TOPIC, MessageField.VALUE))
            if not sensor_data:
                print("⚠️ Mensaje vacío recibido.")
                continue
            
            decompressed_size = sys.getsizeof(avro_deserializer)
            print(f"📦 Tamaño del mensaje descomprimido: {decompressed_size} bytes")

            print(f"✅ Mensaje recibido: {json.dumps(sensor_data, indent=2)}")

            # 🔹 Intentar insertar datos en PostgreSQL
            try:
                insert_data(
                    cursor,
                    sensor_data["sensor_id"],
                    sensor_data["timestamp"],
                    sensor_data["temperatura"],
                    sensor_data["humedad"],
                    sensor_data["presion"]
                )
                conn.commit()  # 🔹 Confirmar transacción
                print("💾 Datos insertados en PostgreSQL.")

            except psycopg2.Error as e:
                conn.rollback()  # 🔹 Revertir cambios en caso de error
                print(f"❌ Error insertando en PostgreSQL: {e}")

        except Exception as e:
            print(f"❌ Error deserializando mensaje Avro: {e}")

except KeyboardInterrupt:
    print("\n🛑 Deteniendo el consumidor...")
# icono de disco flexible


finally:
    print("🔄 Cerrando conexiones...")
    consumer.close()
    cursor.close()
    conn.close()
    print("✅ Conexiones cerradas correctamente.")
