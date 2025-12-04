import os
import pendulum
from airflow import DAG
from datetime import timedelta

from dbt_operator import DbtOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from python_scripts.train_model import process_iris_data

ANALYTICS_DB = os.getenv('ANALYTICS_DB', 'analytics')
PROJECT_DIR = os.getenv('AIRFLOW_HOME') + "/dags/dbt/homework"
PROFILE = 'homework'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

env_vars = {
    'ANALYTICS_DB': ANALYTICS_DB,
    'DBT_PROFILE': PROFILE
}

dbt_vars = {
    'is_test': False,
    'data_date': '{{ ds }}',
}

local_tz = pendulum.timezone("Europe/Kiev")

dag = DAG(
    'process_iris',
    default_args=default_args,
    description='ML pipeline for iris data.',
    start_date=pendulum.datetime(2025, 4, 22, 1, 0, tz=local_tz),
    # end_date=pendulum.datetime(2025, 4, 24, 1, 0, tz=local_tz),
    schedule='0 1 * * *',
    catchup=True,
    tags=['dbt', 'custom'],
    template_searchpath=['/opt/airflow/dags/sql_scripts']
)

start = DummyOperator(
    task_id='start',
    dag=dag,
    doc="Starting ml pipeline for iris data.",
)

setup_privileges = PostgresOperator(
    task_id='setup_privileges',
    postgres_conn_id='postgres_analytics',
    sql='setup_privileges.sql',
    dag=dag,
)

dbt_seed = DbtOperator(
    task_id='dbt_seed',
    dag=dag,
    command='seed',
    profile=PROFILE,
    project_dir=PROJECT_DIR,
    env_vars=env_vars,
    vars=dbt_vars
)

create_staging = DbtOperator(
    task_id='create_staging',
    dag=dag,
    command='run',
    profile=PROFILE,
    project_dir=PROJECT_DIR,
    models=['staging'],
    env_vars=env_vars,
    vars=dbt_vars,
)

create_mart = DbtOperator(
    task_id='create_mart',
    dag=dag,
    command='run',
    profile=PROFILE,
    project_dir=PROJECT_DIR,
    models=['mart'],
    env_vars=env_vars,
    vars=dbt_vars,
)

process_data = PythonOperator(
    task_id='iris_ml_processor',
    dag=dag,
    python_callable=process_iris_data,
    provide_context=True,
)

notify_success = DummyOperator(
    task_id='notify_success',
    dag=dag,
    doc="Emulates email notification about successful DAG run. I have no "
        "e-mail I can use in learning projects that's why I'm using this dummy operator."
        "I know how to use it properly, but I don't want to allow access to my e-mail account.",
)

end = DummyOperator(
    task_id='end',
    dag=dag,
    doc="Ml pipeline for iris data finished successfully.",
)

(start >> setup_privileges >> dbt_seed >> create_staging
 >> create_mart >> process_data >> notify_success >> end)
