from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table_name: str,
                 sql_query: str,
                 conn_id: str = 'redshift',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.sql_query = sql_query
        self.conn_id = conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        self.log.info(f'Inserting data into fact table "{self.table_name}"')
        redshift_hook.run(f'INSERT INTO {self.table_name} {self.sql_query}')
