from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import snowflake.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
import os
from dotenv import load_dotenv
load_dotenv()  # Load variables from .env



# ------------------ EXTRACT ------------------ #
def extract():
    url = "https://api.open-meteo.com/v1/forecast?latitude=40.73&longitude=-73.93&current_weather=true"
    response = requests.get(url)
    data = response.json()

    with open("/tmp/weather.json", "w") as f:
        json.dump(data, f)


# ------------------ TRANSFORM (PySpark) ------------------ #
def transform():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, upper
    import pandas as pd

    spark = SparkSession.builder.appName("WeatherTransform").master("local[*]").getOrCreate()

    # Use full file path
    df = spark.read.json("file:///tmp/weather.json")  # <-- IMPORTANT: add file:///

    weather_df = df.select("current_weather.*")

    transformed_df = weather_df.withColumn(
        "weather_upper",
        upper(col("weathercode").cast("string"))
    )

    # Convert to pandas for Snowflake
    pandas_df = transformed_df.toPandas()
    pandas_df.to_json("/tmp/transformed_weather.json", orient="records")

    spark.stop()


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

    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS WEATHER_DATA (
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode STRING,
            time STRING,
            weather_upper STRING
        )
    """)

    # Load data
    with open("/tmp/transformed_weather.json") as f:
        data = json.load(f)

    for row in data:
        cursor.execute(
            """
            INSERT INTO WEATHER_DATA 
            (temperature, windspeed, winddirection, weathercode, time, weather_upper)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                row.get("temperature"),
                row.get("windspeed"),
                row.get("winddirection"),
                str(row.get("weathercode")),
                row.get("time"),
                row.get("weather_upper")
            )
        )

    conn.close()


# ------------------ DAG ------------------ #
with DAG(
    dag_id = 'weather_etl_spark',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='extract', python_callable=extract)
    t2 = PythonOperator(task_id='transform_spark', python_callable=transform)
    t3 = PythonOperator(task_id='load', python_callable=load)

    t1 >> t2 >> t3