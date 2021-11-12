from typing import List

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table_names: List[str],
                 conn_id: str = 'redshift',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_names = table_names
        self.conn_id = conn_id

    def execute(self, context):
        redshift_hook = PostgresHook('redshift')
        
        for table_name in self.table_names:
            q = f'SELECT COUNT(*) FROM {table_name}'
            num_records = redshift_hook.run(q)
            
            if (
                num_records is None
                and num_records < 1
            ):
                self.log.warn(f'No records in the table "{table_name}"!')
