from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv
import snowflake.connector

default_args = {
    'owner': 'rushi'
}

def extract_data():
    data = [
        (1, "Rushi", "100"),
        (2, "Emma", "250"),
        (3, "David", "500")
    ]

    with open('/tmp/sales.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerows(data)

def load_to_snowflake():
    conn = snowflake.connector.connect(
        user='Rushi',
        password='Snowflakes@6767',
        account='utlfbzv-bf44405',
        warehouse='COMPUTE_WH',
        database='RUSHI_DB',
        schema='PRACTICE'
    )

    cursor = conn.cursor()

    cursor.execute("""
        COPY INTO sales_raw
        FROM @~/sales.csv
        FILE_FORMAT = (TYPE = CSV)
    """)

    conn.close()

def transform_data():
    conn = snowflake.connector.connect(
        user='Rushi',
        password='Snowflakes@6767',
        account='utlfbzv-bf44405',
        warehouse='COMPUTE_WH',
        database='RUSHI_DB',
        schema='PRACTICE'
    )

    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO sales_clean
        SELECT
            id,
            name AS customer,
            amount::NUMBER(10,2),
            CURRENT_TIMESTAMP()
        FROM sales_raw;
    """)

    conn.close()

with DAG(
    'sales_etl_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    load = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    extract >> load >> transform