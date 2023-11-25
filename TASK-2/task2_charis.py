from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
import json

# Instantiate the DAG
with DAG(
    dag_id='gender_prediction_dag',
    description='A DAG to predict gender from JSON file and ingest into PostgreSQL',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:
    
    start = DummyOperator(task_id="start")
    
    predict_gender_task = SimpleHttpOperator(
        task_id="predict_gender",
        method="POST",
        http_conn_id="gender_api",
        endpoint="/gender/by-first-name-multiple",  
        data='[{"first_name": "sandra", "country": "US"},{"first_name": "charis", "country": "US"}]', 
        log_response=True,
    )
    
    create_table_in_db_task = PostgresOperator(
        task_id='create_table_in_db',
        sql=('CREATE TABLE IF NOT EXISTS gender_name_prediction ' +
             '(' +
             'Input TEXT,' +
             'details TEXT,' +
             'result_found BOOLEAN, ' +
             'first_name TEXT, ' +
             'probability FLOAT, ' +
             'gender TEXT, ' +
             'timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP' +
             ')'),
        postgres_conn_id='pg_conn_id',
        autocommit=True,
    )
    
    def load_data_to_postgres(ti=None):
        response = ti.xcom_pull(task_ids='predict_gender',key=None)
        # print(response)
        data_to_insert = json.loads(response)
        # print(data_to_insert)
        # # print(type(data_to_insert))
        insert = """
                INSERT INTO gender_name_prediction
                (input, details, result_found, first_name, probability, gender)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
        # Command untuk proses data ke PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='pg_conn_id').get_conn()
        with pg_hook.cursor() as cursor:
            # Proses dan ambil data yang ingin dimasukkan ke PostgreSQL
            for data in data_to_insert:
                input = json.dumps(data['input'])
                details=json.dumps(data['details'])
                result_found=data['result_found']
                first_name=data['first_name']
                probability=data['probability']
                gender=data['gender']
                
                cursor.execute(insert,(input, details, result_found, first_name, probability, gender))
        pg_hook.commit()
        pg_hook.close()
                
    load_data_to_db_task = PythonOperator(
        task_id='load_data_to_db',
        python_callable=load_data_to_postgres
    )
    
    end = DummyOperator(task_id="end")
    
start >> predict_gender_task >> create_table_in_db_task >> load_data_to_db_task >> end
