from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

default_args = {
    'owner': 'rushi',
    'start_date': datetime(2026, 3, 21),
}

dag = DAG(
    dag_id='crypto_100_etl',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

# -----------------------------
# STEP 1: EXTRACT
# -----------------------------
def extract():
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=100&page=1"
    response = requests.get(url)
    data = response.json()

    # Save raw data
    with open("/tmp/raw.json", "w") as f:
        json.dump(data, f)

# -----------------------------
# STEP 2: TRANSFORM
# -----------------------------
def transform():
    with open("/tmp/raw.json") as f:
        data = json.load(f)

    transformed = []
    for coin in data:
        transformed.append({
            "id": coin["id"],
            "symbol": coin["symbol"],
            "name": coin["name"],
            "price": coin["current_price"],
            "market_cap": coin["market_cap"],
            "volume": coin["total_volume"]
        })

    # Save transformed data
    with open("/tmp/clean.json", "w") as f:
        json.dump(transformed, f)

# -----------------------------
# STEP 3: LOAD
# -----------------------------
def load():
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema=os.environ['SNOWFLAKE_SCHEMA']
    )

    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CRYPTO_PRICE (
            id STRING,
            symbol STRING,
            name STRING,
            price FLOAT,
            market_cap FLOAT,
            volume FLOAT
        )
    """)

    # Insert records
    with open("/tmp/clean.json") as f:
        data = json.load(f)

    for coin in data:
        cursor.execute("""
            INSERT INTO CRYPTO_PRICE (id, symbol, name, price, market_cap, volume)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            coin["id"],
            coin["symbol"],
            coin["name"],
            coin["price"],
            coin["market_cap"],
            coin["volume"]
        ))

    conn.commit()
    conn.close()

# -----------------------------
# DAG TASKS
# -----------------------------
t1 = PythonOperator(task_id='extract', python_callable=extract, dag=dag)
t2 = PythonOperator(task_id='transform', python_callable=transform, dag=dag)
t3 = PythonOperator(task_id='load', python_callable=load, dag=dag)

t1 >> t2 >> t3