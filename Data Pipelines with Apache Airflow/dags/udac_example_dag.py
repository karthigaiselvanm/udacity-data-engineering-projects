from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import ( StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from sparkify_dimtables_subdag import load_dimension_subdag
from airflow.operators.subdag_operator import SubDagOperator


AWS_KEY = AwsHook('aws_credentials').get_credentials().access_key
AWS_SECRET = AwsHook('aws_credentials').get_credentials().secret_key

#Setting variables for S3 bucket

s3_bucket = 'udacity-dend'
song_s3_key = "song_data"
log_s3_key = "log-data"
log_json_file = "log_json_path.json"

#Setting default arguments for Airflow DAG

default_args = {
    'owner': 'karthigaiselvan',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'catchup': True
}

#Creating a DAG for Data pipelines in Airflow
dag_name = 'udac_example_dag' 
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1          
        )

dq_checks = [
    {'check_sql': "SELECT COUNT(*) FROM dim_users WHERE userid is null",
     'expected_result': 0,
     'descr': "null values in dim_users.userid column"},
    {'check_sql': "SELECT COUNT(*) FROM dim_songs WHERE songid is null",
     'expected_result': 0,
     'descr': "null values in dim_songs.songid column"},
    {'check_sql': "SELECT COUNT(*) FROM(SELECT cnt, count(*) FROM (SELECT "
        "songid, count(*) cnt FROM dim_songs GROUP BY songid) GROUP BY cnt)",
     'expected_result': 1,
     'descr': "duplicate song ids have been found"},
    {'dual_sql1': "SELECT COUNT(*) songs_cnt FROM dim_songs",
     'dual_sql2': "SELECT COUNT(DISTINCT song_id) st_sng_song_cnt "
        "FROM stage_songs",
     'descr': "# records in songs table and # DISTINCT stage_songs records"}
]

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    table_name='stage_events',
    s3_bucket=s3_bucket,
    s3_path='log_data',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    region="us-west-2",
    copy_json_option='s3://' + s3_bucket + '/log_json_path.json'
)



stage_songs_to_redshift = StageToRedshiftOperator(
   task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    table_name='stage_songs',
    s3_bucket=s3_bucket,
    s3_path='song_data',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    region="us-west-2",
    copy_json_option='auto'
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.songplay_table_insert, 
    dag=dag
)


load_user_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name=dag_name,
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.user_table_insert,
        append_load=False,
        table_name = "dim_users",
    ),
    task_id="Load_user_dim_table",
    dag=dag,
)


load_song_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name=dag_name,
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.song_table_insert,
        append_load=False,
        table_name = "dim_songs",
    ),
    task_id="Load_song_dim_table",
    dag=dag,
)


load_artist_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name=dag_name,
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.artist_table_insert,
        append_load=False,
        table_name = "dim_artists",
    ),
    task_id="Load_artist_dim_table",
    dag=dag,
)


load_time_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name=dag_name,
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.time_table_insert,
        append_load=False,
        table_name = "dim_time",
    ),
    task_id="Load_time_dim_table",
    dag=dag,
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    checks=dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Flow of DAGs

start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift

stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks 
run_quality_checks >> end_operator