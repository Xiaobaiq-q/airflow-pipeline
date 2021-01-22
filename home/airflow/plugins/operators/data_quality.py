from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs): 

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables=tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Running test")
        for i in self.tables:
            self.log.info(f "Data quality check failed on {self.table}")
            records = redshift_hook.get_records(f "SELECT COUNT(*) FROM {i}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f "Data quality check failed. {i} is null")
            num_record = records[0][0]
            if num_record < 1:
                raise ValueError(f "Data quality check failed. There are 0 rows in {i}")
            self.log.info(f "{i} pass data quality check with {num_records} records ")
        