from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os
from dotenv import load_dotenv
import snowflake.connector
import json

load_dotenv()  # Load variables from .env


# ------------------ EXTRACT ------------------ #
def extract():
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    data = response.json()

    with open("/tmp/api.json", "w") as f:
        json.dump(data, f)


# ------------------ TRANSFORM ------------------ #
def transform():
    with open("/tmp/api.json") as f:
        data = json.load(f)

    transformed = [
        {
            "id": item["id"],
            "title": item["title"].upper(),
            "body": item["body"]
        }
        for item in data
    ]

    with open("/tmp/transformed.json", "w") as f:
        json.dump(transformed, f)


# ------------------ LOAD ------------------ #
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

    # ✅ Step 1: Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS API_DATA (
            id INT,
            title STRING,
            body STRING
        )
    """)

    # ✅ Step 2: Load data
    with open("/tmp/transformed.json") as f:
        data = json.load(f)

    for row in data:
        cursor.execute(
            "INSERT INTO API_DATA (id, title, body) VALUES (%s, %s, %s)",
            (row['id'], row['title'], row['body'])
        )

    conn.close()


# ------------------ DAG ------------------ #
with DAG(
    'api_etl',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id='load',
        python_callable=load
    )

    t1 >> t2 >> t3