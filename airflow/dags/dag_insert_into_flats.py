from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

def insert_data_to_flats():

    # Создаем экземпляр PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgresql',
                           port=5432
                           )

    # Запрос на вставку данных
    sql_query = """INSERT INTO test.flats (data_pars, id_flats, area, floor, price, rooms, status , finish_type, meter_price, project_id)
                    SELECT data_pars, id_flats, area, floor, price, rooms, status , finish_type, meter_price, project_id
                    FROM staging.tyt;"""

    # Добавление данных в таблицу flats
    pg_hook.run(sql_query)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1
}

with DAG('dag_insert_into_flats'
        , default_args=default_args
        , schedule_interval='@daily'
        , catchup=False) as dag:

    load_data_task = PythonOperator(
        task_id='insert_data_to_flats',
        python_callable=insert_data_to_flats
    )

    insert_data_to_flats