from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import task,dag

# Instantiate the DAG
with DAG(
    'Trial_Task_1',
    description='This task is for Run DAG every 5 hours, push to xcom and pull multiple value from xcom',
    schedule_interval='0 */5 * * *',  # Run every 5 hours
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:
    start = DummyOperator(task_id="start")
# Define a multiple Python function to push variable to XCom
    @task
    def push_1(ti=None):
       values = 'Halo ini tugas charis'
       ti.xcom_push(key='key', value=values)
       print (f'Push Variable 1 : {values}')
    
    @task
    def push_2(ti=None):
        values2 = 'Tugas ini Sangat menarik'
        ti.xcom_push(key='key', value=values2)
        print (f'Push Variable 2 : {values2}')
        
    @task
    def push_3(ti=None):
        values3 = 'Saya suka DE'
        ti.xcom_push(key='key', value=values3)
        print (f'Push Variable 3 : {values3}')

# Define a Python function to pull multiple values from XCom
    @task
    def pull_values_from_xcom(ti=None):
        result = ti.xcom_pull(task_ids=['push_1','push_2','push_3'],key='key')
        print(f'Pull variable from xcom: {result}')
    
    end = DummyOperator(task_id="end")
    
# Set the task dependencies
    start >> [push_1(),push_2(),push_3()] >> pull_values_from_xcom() >> end
    


