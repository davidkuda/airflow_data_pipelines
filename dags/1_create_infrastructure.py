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


def create_infrastructure():
    aws = create_aws_connection()
    aws.create_infrastructure()


def set_redshift_endpoint_as_var():
    aws = create_aws_connection()
    redshift_url = aws.get_dwh_endpoint()
    Variable.set(key='REDSHIFT_URL', value=redshift_url)
    print(f'Set the variable REDSHIFT_URL to "{redshift_url}"')


def set_iam_role_arn_as_var():
    aws = create_aws_connection()
    iam_role_arn = aws.get_iam_role_arn()
    Variable.set(key='DWH_ROLE_ARN', value=iam_role_arn)
    print(f'Set the variable DWH_ROLE_ARN to "{iam_role_arn}"')


with DAG(
    '1_create_infrastructure',
    default_args=default_args,
    description='Create a redshift cluster on AWS',
    schedule_interval='0 * * * *',
    catchup=False
) as dag:
    create_aws_infrastructure = PythonOperator(
        task_id='create_aws_infrastructure',
        python_callable=create_infrastructure
    )
    
    set_var_redshift_url = PythonOperator(
        task_id='set_var_redshift_url',
        python_callable=set_redshift_endpoint_as_var
    )

    set_var_dwh_role_arn = PythonOperator(
        task_id='set_var_dwh_role_arn',
        python_callable=set_iam_role_arn_as_var
    )

    create_aws_infrastructure >> [set_var_redshift_url, set_var_dwh_role_arn]
