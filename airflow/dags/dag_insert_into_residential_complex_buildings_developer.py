from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

def insert_data_to_residential_complex():

    # Создаем экземпляр PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgresql',
                           port=5432
                           )

    # Запрос на вставку данных
    sql_query = """INSERT INTO test.residential_complex (project_id, metro, name_project, url_project, longitude, latitude, locations, id_developer)
                    SELECT distinct project_id, metro, name_project, url_project, longitude, latitude, locations, 1 FROM staging.tyt
                    WHERE project_id NOT IN (SELECT project_id FROM test.residential_complex);"""

    # Добавление данных в таблицу flats
    pg_hook.run(sql_query)

def insert_data_to_buildings():

    # Создаем экземпляр PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgresql',
                           port=5432
                           )

    # Запрос на вставку данных
    sql_query = """INSERT INTO test.buildings (project_id, max_floor, deadline, korpus)
                    SELECT distinct project_id, max_floor, deadline, korpus FROM staging.tyt
                    WHERE project_id NOT IN (SELECT project_id FROM test.buildings)
                        AND korpus NOT IN (SELECT korpus FROM test.buildings)
                        AND max_floor NOT IN (SELECT max_floor FROM test.buildings);"""

    # Добавление данных в таблицу flats
    pg_hook.run(sql_query)

def insert_data_to_developer():

    # Создаем экземпляр PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgresql',
                           port=5432
                           )

    # Запрос на вставку данных
    sql_query = """INSERT INTO test.developer (id_developer, developer)
                    SELECT DISTINCT id_developer, developer from  staging.tyt
                    WHERE developer NOT IN (SELECT DISTINCT developer from test.developer);"""

    # Добавление данных в таблицу flats
    pg_hook.run(sql_query)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1
}

with DAG('dag_insert_into_residential_complex_buildings_developer', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    load_data_task_residential_complex = PythonOperator(
        task_id='insert_data_to_rbd',
        python_callable=insert_data_to_residential_complex
    )

    load_data_task_buildings = PythonOperator(
        task_id='insert_data_to_buildings',
        python_callable=insert_data_to_buildings
    )

    load_data_task_developer = PythonOperator(
        task_id='insert_data_to_developer',
        python_callable=insert_data_to_developer
    )

    load_data_task_residential_complex >> load_data_task_buildings >> load_data_task_developer