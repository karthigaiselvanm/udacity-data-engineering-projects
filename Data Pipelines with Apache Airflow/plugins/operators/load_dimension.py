from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query = "",
                 append_load = "",
                 table_name = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table_name = table_name
        self.append_load = append_load


    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.append_load == True:
            self.log.info(f"Append load is set True, running query to load data into Dimension Table {self.table_name}")
            redshift_hook.run(self.sql_query)
            self.log.info(f"Dimension Table {self.table_name} loaded.")
        else:
            self.log.info(f"Append load is set to False,running query to delete data from Dimension Table {self.table_name}")
            sql_statement = 'DELETE FROM %s' % self.table_name
            redshift_hook.run(sql_statement)

            self.log.info(f"Running query to load data into Dimension Table {self.table_name}")
            redshift_hook.run(self.sql_query)
            self.log.info(f"Dimension Table {self.table_name} loaded.")
