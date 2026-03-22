from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract():
    print("Extracting data")
    return [1,2,3,4,5]

def transform():
    print("Transforming data")
    print([x*10 for x in [1,2,3,4,5]])

def validate():
    print("Validating data")
    print("Data is valid ✅")

def load():
    print("Loading data")
    print("Data loaded 💾")

with DAG(
    dag_id = "step3_parallel",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # DAG will run daily
    catchup=False
) as dag:

    task_extract = PythonOperator(task_id="extract", python_callable=extract)
    task_transform = PythonOperator(task_id="transform", python_callable=transform)
    task_validate = PythonOperator(task_id="validate", python_callable=validate)
    task_load = PythonOperator(task_id="load", python_callable=load)

    # Dependencies
    task_extract >> [task_transform, task_validate] >> task_load
