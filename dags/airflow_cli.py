import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable


AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
REDSHIFT_URL = Variable.get('REDSHIFT_URL')


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
    'airflow_cli',
    default_args=default_args,
    description='Create a redshift cluster on AWS',
    schedule_interval='0 * * * *',
    catchup=False
) as dag:
    airflow_info = BashOperator(
        task_id='airflow_info',
        bash_command='airflow info'
    )

    debug = BashOperator(
        task_id='debug',
        bash_command=f'echo {AWS_ACCESS_KEY_ID}'
    )
    
    delete_aws_conn = BashOperator(
        task_id='delete_aws_conn',
        bash_command='airflow connections delete aws_credentials'
    )
    
    add_aws_conn = BashOperator(
        task_id='connect_aws',
        bash_command=f'''
        airflow connections add 'aws_credentials' \
            --conn-type 'Amazon Web Services' \
            --conn-login {AWS_ACCESS_KEY_ID} \
            --conn-password {AWS_SECRET_ACCESS_KEY}
        '''
    )

    delete_redshift_conn = BashOperator(
        task_id='delete_redshift_conn',
        bash_command='airflow connections delete redshift'
    )
    
    add_redshift_conn = BashOperator(
        task_id='connect_redshift',
        bash_command=f"""
        airflow connections add 'redshift' \
            --conn-type 'Postgres' \
            --conn-login 'dwhuser' \
            --conn-password 'Passw0rd' \
            --conn-schema 'dwh' \
            --conn-port '5439' \
            --conn-host '{REDSHIFT_URL}'
        """
    )
    
    
    delete_aws_conn >> add_aws_conn
    delete_redshift_conn >> add_redshift_conn
