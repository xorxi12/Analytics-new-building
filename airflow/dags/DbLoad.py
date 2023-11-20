def load_data_to_postgres(df):
    from airflow.hooks.postgres_hook import PostgresHook
    from sqlalchemy import INTEGER, DATE, FLOAT, VARCHAR, DECIMAL, SMALLINT, BOOLEAN, BIGINT
    import pandas as pd
    schema = {
        'id': INTEGER,
        'data_pars': DATE,
        'id_flats': BIGINT,
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
              if_exists='append',
              chunksize=10000,
              dtype=schema
              )
