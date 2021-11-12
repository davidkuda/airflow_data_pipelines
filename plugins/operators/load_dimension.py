from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table_name: str,
                 sql_query: str,
                 conn_id: str = 'redshift',
                 truncate: bool = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.sql_query = sql_query
        self.conn_id = conn_id
        self.truncate = truncate

    def execute(self, context):

        redshift_hook = PostgresHook(self.conn_id)

        if self.truncate:
            self.log.info(f'Truncating table "{self.table}" before loading data')
            redshift_hook.run(f'TRUNCATE {self.table_name}')
        
        self.log.info(f'Inserting data into dimensions table')
        redshift_hook.run(f'INSERT INTO {self.table_name} {self.sql_query}')
