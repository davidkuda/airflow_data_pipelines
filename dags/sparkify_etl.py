from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                # LoadDimensionOperator, DataQualityOperator)

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET = os.environ.get('AWS_SECRET_ACCESS_KEY_ID')

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
    'sparkify etl',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
) as dag:

    start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks'
    )

    end_operator = DummyOperator(task_id='Stop_execution')
