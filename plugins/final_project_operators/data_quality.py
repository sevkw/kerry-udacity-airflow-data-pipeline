import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    This Operator performs a data quality check if certain column contains 
        NULL values by counting all the rows that have NULL in the column. 
    We do not want to have any NULLs so expected result would be 0 and the test 
        would compare the SQL statement's outcome to the expected result.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            records = redshift_hook.get_records("SELECT COUNT(*) FROM {}".format(table))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. {} returned no results".format(table))
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError("Data quality check failed. {} contained 0 rows".format(table))
            logging.info("Data quality on table {} check passed with {} records".format(table, num_records))
