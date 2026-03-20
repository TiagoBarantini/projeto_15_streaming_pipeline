import streamlit as st
import psycopg2
import pandas as pd
import time
import os

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

st.title("📊 Crypto Streaming Dashboard")

def get_data():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB
    )

    query = "SELECT * FROM crypto_prices ORDER BY created_at DESC LIMIT 50"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

while True:
    df = get_data()

    st.metric("Último preço BTC", f"${df.iloc[0]['price']:.2f}")

    st.line_chart(df.set_index("created_at")["price"])

    time.sleep(5)
    st.rerun()
