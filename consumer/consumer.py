import json
import time
import psycopg2
from kafka import KafkaConsumer
import os

print("🚀 CONSUMER INICIANDO...")

# ⏳ Delay inicial (evita corrida entre containers)
time.sleep(10)

# 🔧 Variáveis com fallback (ESSENCIAL)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "streaming")

TOPIC = "crypto_prices"


# 🔌 Conexão com Postgres (com retry)
def connect_db():
    while True:
        try:
            print("⏳ Tentando conectar no Postgres...")
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=POSTGRES_DB
            )
            print("✅ Conectado ao Postgres")
            return conn
        except Exception as e:
            print(f"❌ Erro ao conectar no Postgres: {e}")
            time.sleep(3)


# 🔌 Conexão com Kafka (com retry)
def connect_kafka():
    while True:
        try:
            print("⏳ Tentando conectar no Kafka...")
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id="crypto-group"
            )
            print("✅ Conectado ao Kafka")
            return consumer
        except Exception as e:
            print(f"❌ Erro ao conectar no Kafka: {e}")
            time.sleep(3)


# 🔗 Conectar serviços
conn = connect_db()
cursor = conn.cursor()

consumer = connect_kafka()

print("🔄 Iniciando consumo de mensagens...")


# 🔄 Loop principal
for message in consumer:
    data = message.value
    print(f"📥 Recebido: {data}")

    try:
        cursor.execute(
            "INSERT INTO crypto_prices (symbol, price) VALUES (%s, %s)",
            (data["symbol"], data["price"])
        )
        conn.commit()
        print("💾 Inserido no banco com sucesso")
    except Exception as e:
        print(f"❌ Erro ao inserir no banco: {e}")
        conn.rollback()
