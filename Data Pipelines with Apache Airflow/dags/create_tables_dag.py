from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator
from helpers.create_tables import create_all_tables

#Setting up the default arguments for DAG

default_args = {
    'owner': 'karthigaiselvan',
    'start_date': datetime.utcnow() - timedelta(hours=5),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'catchup_by_default': False,
}

#Creating a separate DAG for creating tables in Redshift

dag = DAG('create_tables_dag',
          default_args=default_args,
          description='Creating tables in Redshift',
          schedule_interval=None,
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_all_tables = PostgresOperator(
    task_id="create_all_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_all_tables
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Flow of the DAGs

start_operator >> create_all_tables
create_all_tables >> end_operator