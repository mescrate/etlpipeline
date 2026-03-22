from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# --- Step 1: Extract ---
def extract():
    print("Step 1: Extracting data")
    data = [1, 2, 3, 4, 5]  # Example: pretend we fetched this
    return data  # Optional: just for learning

# --- Step 2: Transform ---
def transform():
    print("Step 2: Transforming data")
    transformed = [x * 10 for x in [1,2,3,4,5]]  # Example transform
    print("Transformed data:", transformed)

# --- Step 3: Load ---
def load():
    print("Step 3: Loading data")
    print("Data loaded to database (simulated)")

# --- DAG definition ---
with DAG(
    dag_id="step2_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # No auto schedule yet
    catchup=False
) as dag:

    task_extract = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    task_load = PythonOperator(
        task_id="load",
        python_callable=load
    )

    # --- Set task order ---
    task_extract >> task_transform >> task_load
