from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 9, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'create_tables',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
) as dag:
     
    drop_tables = PostgresOperator(
        task_id='drop_tables',
        postgres_conn_id='redshift',
        sql='sql/drop_tables.sql'
    )

    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='redshift',
        sql='sql/create_tables.sql'
    )

    drop_tables >> create_tables
