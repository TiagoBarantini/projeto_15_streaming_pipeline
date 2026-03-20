FROM python:3.11

WORKDIR /app

COPY app.py .

RUN pip install streamlit psycopg2-binary pandas

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
