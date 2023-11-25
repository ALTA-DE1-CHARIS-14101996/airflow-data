from datetime import datetime
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

# Instantiate the DAG
with DAG(
    dag_id='gender_prediction_dag2',
    description='A DAG to predict gender from gender-api and ingest into PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:
    
    # Task to predict multiple names from gender-api
    predict_gender_task = SimpleHttpOperator(
        task_id="predict_gender",
        method="POST",
        http_conn_id="gender_api",
        endpoint="/gender/by-first-name-multiple",  
        data='[{"first_name": "sandra", "country": "US"},{"first_name": "charis", "country": "US"}]', 
        log_response=True,
    )

    # Task to create table in PostgreSQL
    create_table_task = PostgresOperator(
        task_id='create_table',
        sql=('''
            CREATE TABLE IF NOT EXISTS gender_name_prediction (
                input TEXT,
                details TEXT,
                result_found BOOLEAN,
                first_name TEXT,
                probability FLOAT,
                gender TEXT,
                timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        '''),
        postgres_conn_id='pg_conn_id', 
        autocommit=True,
    )

    # Task to load prediction results to the PostgreSQL table
    def load_data_to_postgres(ti=None):
        response = ti.xcom_pull(task_ids='predict_gender')

        if response:
            # Additional processing if needed
            data_to_insert = response

            pg_hook = PostgresHook(postgres_conn_id='pg_conn_id')  # Replace with the actual connection ID
            with pg_hook.get_conn().cursor() as cursor:
                cursor.executemany("""
                    INSERT INTO gender_name_prediction
                    (input, details, result_found, first_name, probability, gender)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, data_to_insert)
                pg_hook.commit()

    load_data_task = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres,
    )

    # Define the task dependencies
    predict_gender_task >> create_table_task >> load_data_task
