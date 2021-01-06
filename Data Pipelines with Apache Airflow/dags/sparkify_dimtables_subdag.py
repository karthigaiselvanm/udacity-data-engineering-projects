from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import LoadDimensionOperator
from helpers import SqlQueries

#Defining the subdag for loading all the dimension tables

def load_dimension_subdag(
    parent_dag_name,
    task_id,
    redshift_conn_id,
    sql_statement,
    table_name,
    append_load,
    *args, **kwargs):
    
    dag = DAG(f"{parent_dag_name}.{task_id}", **kwargs)
    
    load_dimension_table = LoadDimensionOperator(
        task_id=task_id,
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        sql_query = sql_statement,
        append_load=append_load,
        table_name = table_name,
    )    
    
    load_dimension_table
    
    return dag