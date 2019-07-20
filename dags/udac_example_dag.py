# from datetime import datetime,timedelta
import os
import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadDimensionOperator,LoadFactOperator, DataQualityOperator)
from helpers import SqlQueries


start_date = datetime.datetime(2019,1,12)
default_args = {
    'owner': 'udacity',
    'start_date': start_date
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json",
    data_format = "JSON",
    execution_date = "{execution_date}"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    json_path = "auto",
    data_format = "JSON"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = "songplays",
    redshift_conn_id = "redshift",
    sql = SqlQueries.songplay_table_insert
)

load_songs_dim_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.song_table_insert
)

load_users_dim_table = LoadDimensionOperator(
    task_id = 'Load_users_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.user_table_insert
)

load_artists_dim_table = LoadDimensionOperator(
    task_id = 'Load_artists_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.artist_table_insert
)

load_times_dim_table = LoadDimensionOperator(
    task_id = 'Load_times_dim_table',
    dag=dag,
    table='times',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.time_table_insert
)
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ['songplays','songs','users','artists','times']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table


load_songplays_table >> load_songs_dim_table
load_songplays_table >> load_users_dim_table
load_songplays_table >> load_artists_dim_table
load_songplays_table >> load_times_dim_table

load_songs_dim_table >> run_quality_checks
load_users_dim_table >> run_quality_checks
load_artists_dim_table >> run_quality_checks
load_times_dim_table >> run_quality_checks

run_quality_checks >> end_operator

