from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from sql.insert_queries import SqlQueries

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
    '4_sparkify_etl',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
) as dag:

    start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events',
        table_name='public.staging_events',
        s3_prefix='log-data'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs',
        table_name='public.staging_songs',
        s3_prefix='song-data'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table_name='public.songplays',
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_users_dim_table',
        table_name='public.users',
        sql_query=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table_name='public.songs',
        sql_query=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table_name='public.artists',
        sql_query=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table_name='public.time',
        sql_query=SqlQueries.time_table_insert
    )

    table_names = [
        'public.artists',
        'public.songplays',
        'public.songs',
        'public.staging_events',
        'public.staging_songs',
        'public.time',
        'public.users'
    ]

    run_quality_checks = DataQualityOperator(
        table_names=table_names,
        task_id='Run_data_quality_checks'
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] \
        >> load_songplays_table \
        >> [load_song_dimension_table,
            load_time_dimension_table,
            load_user_dimension_table,
            load_artist_dimension_table] \
        >> run_quality_checks \
        >> end_operator
