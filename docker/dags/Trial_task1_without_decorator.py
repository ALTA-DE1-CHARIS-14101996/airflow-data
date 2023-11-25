from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


# Instantiate the DAG
dag = DAG(
    'task1_charis',
    description='This task is for Run DAG every 5 hours, push to xcom and pull multiple value from xcom',
    schedule_interval='0 */5 * * *',  # Run every 5 hours
    start_date=datetime(2023, 1, 1),
    catchup=False
)
start = DummyOperator(task_id="start")

# Define a Python function to push a variable to XCom
def push_variable_to_xcom(**kwargs):
    variable_value = 'Halo ini tugas charis'
    kwargs['ti'].xcom_push(key='task', value=variable_value)
    print(f"Pushed variable to XCom: {variable_value}")
    
    variable_value2= 'Tugas yang sangat menarik'
    kwargs['ti'].xcom_push(key='task2', value=variable_value2)
    print(f"Pushed variable to XCom: {variable_value2}")
    
    variable_value3= 'Saya suka DE'
    kwargs['ti'].xcom_push(key='task3', value=variable_value3)
    print(f"Pushed variable to XCom: {variable_value3}")

# Define a Python function to pull multiple values from XCom
def pull_values_from_xcom(**kwargs):
    ti = kwargs['ti']    
    
# Pulling values from XCom using task instance's context
    pulled_value = ti.xcom_pull(task_ids='push_variable_to_xcom', key='task')
    pulled_value2 = ti.xcom_pull(task_ids='push_variable_to_xcom', key='task2')
    pulled_value3 = ti.xcom_pull(task_ids='push_variable_to_xcom', key='task3')
    print(f"Pulled values from XCom: {pulled_value},{pulled_value2},{pulled_value3}")

# Define the first PythonOperator to push a variable to XCom
push_task = PythonOperator(
    task_id='push_task',
    python_callable=push_variable_to_xcom,
    provide_context=True,
    dag=dag
)

# Define the second PythonOperator to pull multiple values from XCom
pull_task = PythonOperator(
    task_id='pull_task',
    python_callable=pull_values_from_xcom,
    provide_context=True,
    dag=dag
)
end = DummyOperator(task_id="end")

# Set the task dependencies
start >> push_task >> pull_task >> end
