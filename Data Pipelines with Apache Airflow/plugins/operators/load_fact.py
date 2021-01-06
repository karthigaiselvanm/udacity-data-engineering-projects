from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query = "",
                 append_load = False,
                 table_name = "songplays_fact",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.append_load = append_load
        self.table_name = table_name
        

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.append_load == True:
            redshift_hook.run(self.sql_query)
        else:
            self.log.info("Append load is set to False,running query to delete data from Fact Table")
            sql_statement = 'DELETE FROM %s' % self.table_name
            redshift_hook.run(sql_statement)

            self.log.info(f"Running query to load data into Fact Table")
            redshift_hook.run(self.sql_query)
            self.log.info(f"Fact Table has been loaded successfully!!.")