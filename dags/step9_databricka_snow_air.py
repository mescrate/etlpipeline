# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
# from datetime import datetime
# import requests
# import json
# import snowflake.connector
# import os
# from dotenv import load_dotenv

# load_dotenv()  # Load variables from .env


# def extract():
#     url = "https://jsonplaceholder.typicode.com/posts"
#     response = requests.get(url)
#     data = response.json()

#     with open("/tmp/api.json", "w") as f:
#         json.dump(data, f)

# def load():
#     conn = snowflake.connector.connect(
#         user=os.environ['SNOWFLAKE_USER'],
#         password=os.environ['SNOWFLAKE_PASSWORD'],
#         account=os.environ['SNOWFLAKE_ACCOUNT'],
#         warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
#         database=os.environ['SNOWFLAKE_DATABASE'],
#         schema=os.environ['SNOWFLAKE_SCHEMA']
#     )

#     cursor = conn.cursor()

#     with open("/tmp/transformed.json") as f:
#         data = json.load(f)

#     for row in data:
#         cursor.execute(
#             "INSERT INTO api_data (id, title, body) VALUES (%s,%s,%s)",
#             (row["id"], row["title"], row["body"])
#         )

#     conn.close()


# with DAG(
#     'api_etl_databricks',
#     start_date=datetime(2024,1,1),
#     schedule='@daily',
#     catchup=False
# ) as dag:

#     t1 = PythonOperator(
#         task_id='extract',
#         python_callable=extract
#     )

#     databricks_transform = DatabricksSubmitRunOperator(
#         task_id="databricks_transform",
#         databricks_conn_id="databricks_default",
#         json={
#             "new_cluster": {
#                 "spark_version": "13.3.x-scala2.12",
#                 "node_type_id": "i3.xlarge",
#                 "num_workers": 1
#             },
#             "notebook_task": {
#                 "notebook_path": "/Workspace/Integrated_with_airflow_snowflakes"
#             }
#         }
#     )

#     t3 = PythonOperator(
#         task_id='load',
#         python_callable=load
#     )

#     t1 >> databricks_transform >> t3