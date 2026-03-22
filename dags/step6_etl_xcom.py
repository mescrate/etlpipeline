# """
# What is XCom?

# XCom = “Cross-Communication”

# Lets one task send data to another task

# Useful in ETL pipelines: extract → transform → load

# Without XCom, each task is isolated.
# With XCom, transform can use the output from extract.
# """

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# --- Step 1: Extract ---
def extract(ti):  # ti = task instance
    data = [1, 2, 3, 4, 5]
    print("Extracted data:", data)
    ti.xcom_push(key="data", value=data)  # send data to next task

# --- Step 2: Transform ---
def transform(ti):
    data = ti.xcom_pull(key="data", task_ids="extract")  # get data from extract
    transformed = [x * 10 for x in data]
    print("Transformed data:", transformed)
    ti.xcom_push(key="transformed", value=transformed)

# --- Step 3: Load ---
def load(ti):
    transformed = ti.xcom_pull(key="transformed", task_ids="transform")
    print("Loading data:", transformed)
    print("Data loaded to DB (simulated) ✅") 

# --- DAG ---
with DAG(
    dag_id = "step6_etl_xcom",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    task_extract = PythonOperator(task_id="extract", python_callable=extract)
    task_transform = PythonOperator(task_id="transform", python_callable=transform)
    task_load = PythonOperator(task_id="load", python_callable=load)

    task_extract >> task_transform >> task_load
