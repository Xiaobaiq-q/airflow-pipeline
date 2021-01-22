from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    sql_temp=""
    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql='',
                 append_load = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.sql=sql
        self.append_load=append_load

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_load:
            sql_temp="""
            INSERT INTO TABLE {table} AS
            {sql}
            """
        else: 
            sql_temp="""
            DROP TABLE IF EXISTS {table}
            CREATE TABLE {table} AS
            {sql}
            """
        formatted_sql = LoadFactOperator.sql.format(
            table=self.table,
            sql=self.sql
        )
        redshift.run(formatted_sql)
        self.log.info('LoadDimensionOperator is Done')
