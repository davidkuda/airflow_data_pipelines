from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.variable import Variable


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 table_name: str,
                 s3_prefix: str,
                 s3_bucket: str = 'udacity-dend',
                 dwh_role_arn: str = Variable.get('DWH_ROLE_ARN'),
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.dwh_role_arn = dwh_role_arn


    def execute(self, context):
        copy_staging_table_query = f"""
        COPY {self.table_name}
        FROM 's3://{self.s3_bucket}/{self.s3_prefix}'
        CREDENTIALS 'aws_iam_role={self.dwh_role_arn}'
        REGION 'us-west-2'
        JSON 'auto ignorecase';
        """

        redshift_hook = PostgresHook('redshift')
        conn = redshift_hook.get_conn()
        cur = conn.cursor()
        self.log.info(f'Copying data to the redshift table "{self.table_name}"')
        cur.execute(copy_staging_table_query)
        conn.commit()
