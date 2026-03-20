import json
import time
import random
from kafka import KafkaProducer
import os

print("🚀 PRODUCER INICIANDO...")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "crypto_prices"

# 🔌 Conectar no Kafka com retry
def connect_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda x: json.dumps(x).encode("utf-8")
            )
            print("✅ Conectado ao Kafka")
            return producer
        except Exception as e:
            print(f"⏳ Kafka ainda não disponível: {e}")
            time.sleep(3)

producer = connect_kafka()

# 💰 Preço inicial
price = 50000

# 🔄 Loop infinito
while True:
    # simular variação
    price += random.uniform(-100, 100)

    data = {
        "symbol": "BTC",
        "price": round(price, 2)
    }

    producer.send(TOPIC, data)
    print(f"📤 Enviado: {data}")

    time.sleep(2)
