from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

from utils.aws_infrastructure import AWS
from utils.utilities import parse_config_file


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 9, 1),
    'depends_on_past': False,
    'email_on_failure': False,
}


def create_aws_connection():
    return AWS(
        aws_access_key_id=Variable.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'),
        region='us-west-2',
        config_params=parse_config_file()
    )


def destroy_infrastructure():
    aws = create_aws_connection()
    aws.destroy_infrastructure()


with DAG(
    '5_destroy_infrastructure',
    default_args=default_args,
    description='Destroy infrastructure on AWS',
    schedule_interval='0 * * * *',
    catchup=False
) as dag:
    destroy_aws_infrastructure = PythonOperator(
        task_id='destroy_aws_infrastructure',
        python_callable=destroy_infrastructure
    )
