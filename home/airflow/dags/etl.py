from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (LoadFactOperator,StageToRedshiftOperator,
                                LoadDimensionOperator,DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'catchup':False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@hourly'
}

dag = DAG('etl',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables=PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket='udacity-dend',
    s3_key="log_data/",
    extra_params="format as json",
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket='udacity-dend',
    s3_key="song_data/",
    extra_params="format as auto",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    sql=SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    append_load = True,
    sql=SqlQueries.user_table_insert,
    redshift_conn_id="redshift"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    append_load = True,
    sql=SqlQueries.song_table_insert,
    redshift_conn_id="redshift"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    append_load = True,
    sql=SqlQueries.artist_table_insert,
    redshift_conn_id="redshift"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="times",
    append_load = True,
    sql=SqlQueries.time_table_insert,
    redshift_conn_id="redshift"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    tables =['songplays', 'songs', 'artists', 'users', 'time'],
    redshift_conn_id="redshift",
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table 
load_songplays_table >> load_user_dimension_table 
load_songplays_table >> load_artist_dimension_table 
load_songplays_table >> load_time_dimension_table 
load_song_dimension_table  >> run_quality_checks
load_user_dimension_table  >> run_quality_checks
load_artist_dimension_table  >> run_quality_checks
load_time_dimension_table  >> run_quality_checks 
run_quality_checks >> end_operator 