from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_airflow():
    print("Hello Airflow ")

dag = DAG(
    dag_id="step1_hello",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)

task1 = PythonOperator(
    task_id="hello_task",
    python_callable=hello_airflow,
    dag=dag
)
