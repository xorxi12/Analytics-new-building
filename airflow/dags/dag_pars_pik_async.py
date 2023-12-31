from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import asyncio
from sqlalchemy.types import *
from datetime import date, datetime
import aiohttp
import pandas as pd
import time

start_time = time.time()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1
}

async def get_json(session, url):
    async with session.get(url) as response:
        return await response.json()

async def get_cnt(session):
    url = 'https://api-selectel.pik-service.ru/v2/filter?project'
    json_data = await get_json(session, url=url)
    return 19036 # int(json_data['count'])

async def get_apartments(session, page):
    url = f'https://flat.pik-service.ru/api/v1/filter/flat?type=1,2&location=2,3&flatLimit=50&allFlats=1&flatPage={page}'
    json_data = await get_json(session, url)
    items = json_data['data']['items']

    data = []
    total_cnt = set()

    for item in items:
        data_pars = str(date.today())
        id_apartment = item['id']
        area = item['area']
        floor = item['floor']
        metro = item['metro']['name']
        price = item['price']
        rooms = item['rooms']
        status = 1 if item['status'] == 'true' else 0
        maxFloor = item['maxFloor']
        meterPrice = item['meterPrice']
        deadline = item['settlementDate']
        korpus = item['bulkName'].split()[1]
        name_project = item['blockName']
        finishType = item['finishType']
        url = f'https://www.pik.ru/flat/{id_apartment}'

        if id_apartment not in total_cnt:
            data.append([data_pars, id_apartment, area, floor,
                         metro, price, rooms, bool(status), maxFloor,
                         meterPrice, deadline, korpus, name_project,
                         finishType, url])

        total_cnt.add(id_apartment)
    print(f'Page {page} success')

    return data

async def gather_apartments():
    async with aiohttp.ClientSession(trust_env=True) as session:
        cnt = await get_cnt(session)

        tasks = []
        for page in range(1, (cnt // 20) + 1):
            task = asyncio.create_task(get_apartments(session, page))
            tasks.append(task)

        results = await asyncio.gather(*tasks)
        data = [item for sublist in results for item in sublist]
        print(f'Ко-во квартир = {cnt}, коли-во полученных кв = {len(data)}')
        return data

async def get_projects(session):
    url = 'https://api-selectel.pik-service.ru/v2/filter?project&onlyBlocks=1'
    json_data = await get_json(session, url)

    data = []
    for project in json_data:
        url_project = project['path']
        project_id = project['id']
        longitude = project['longitude']
        latitude = project['latitude']
        name_project = project['name']
        locations = project['locations']['child']['name']
        developer = 'ПИК'
        id_developer = 1
        data.append([url_project, project_id, longitude, latitude, name_project, locations, developer, id_developer])

    return data

async def main():
    async with aiohttp.ClientSession(trust_env=True) as session:
        apartments = await gather_apartments()
        projects = await get_projects(session)

        # Дальнейшая обработка данных
        # Например, создание DataFrame с помощью pandas
        df_apartments = pd.DataFrame(apartments, columns=['data_pars', 'id_apartment', 'area', 'floor', 'metro', 'price', 'rooms', 'status', 'maxFloor', 'meterPrice', 'deadline', 'korpus', 'name_project', 'finishType', 'url'])
        df_projects = pd.DataFrame(projects, columns=['url_project', 'project_id', 'longitude', 'latitude', 'name_project', 'locations', 'developer', 'id_developer'])

        # Объединяем данных
        df = pd.merge(df_apartments, df_projects, on='name_project', how='left')

        json_string = df.to_json()

    finish_time = time.time() - start_time
    print(f'Время работы скрипта {finish_time}')

    return json_string

def run():
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(main())
    return result

def load_data_to_postgres(json_string):

    df = pd.read_json(json_string)

    columns = ['data_pars', 'id_flats', 'area',
               'floor', 'metro', 'price', 'rooms',
               'status', 'max_floor', 'meter_price',
               'deadline', 'korpus', 'name_project',
               'finish_type', 'url', 'url_project',
               'project_id', 'longitude', 'latitude',
               'locations', 'developer', 'id_developer']

    df = df.rename(columns=dict(zip(df.columns, columns)))

    schema = {
        'id': INTEGER,
        'data_pars': DATE,
        'id_flats': INTEGER,
        'area': FLOAT,
        'floor': INTEGER,
        'metro': VARCHAR(100),
        'price': DECIMAL(10, 2),
        'rooms': SMALLINT,
        'status': BOOLEAN,
        'max_floor': SMALLINT,
        'meter_price': DECIMAL(10, 2),
        'deadline': VARCHAR(20),
        'korpus': VARCHAR(20),
        'name_project': VARCHAR(50),
        'finish_type': INTEGER,
        'url': VARCHAR(100),
        'url_project': VARCHAR(100),
        'project_id': INTEGER,
        'longitude': DECIMAL(9, 6),
        'latitude': DECIMAL(8, 6),
        'locations': VARCHAR(100),
        'developer': VARCHAR(100),
        'id_developer': SMALLINT
    }
    pg_hook = PostgresHook(postgres_conn_id='postgresql', port=5432)

    df.to_sql('tyt',
              pg_hook.get_sqlalchemy_engine(),
              schema='staging',
              if_exists='replace',
              chunksize=10000,
              dtype=schema
              )

with DAG('dag_pars_pik_async_1',
          default_args=default_args,
          schedule_interval='@daily',
          catchup=False) as dag:

    run_task = PythonOperator(
        task_id='run_task',
        python_callable=run,
        dag=dag
    )

    load_data_task = PythonOperator(
        task_id='load_bd_dag',
        python_callable=load_data_to_postgres,
        op_args=[run()]
    )

run_task >> load_data_task