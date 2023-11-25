from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd

# Instantiate the DAG
with DAG(
    dag_id='cobacoba',
    description='A DAG to predict gender from JSON file and ingest into PostgreSQL',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:
    
    start = DummyOperator(task_id="start")
    
    @task
    def respone(ti=None):
        response = ti.xcom_pull(task_ids='post_name')
        print (response)
    
    end = DummyOperator(task_id="end")
    
start>> respone() >> end
