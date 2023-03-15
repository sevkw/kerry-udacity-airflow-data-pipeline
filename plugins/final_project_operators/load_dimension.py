from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from udacity.common.final_project_sql_statements import SqlQueries
from airflow.secrets.metastore import MetastoreBackend

class LoadDimensionOperator(BaseOperator):
    """
    This Operator helps loading data into a target Dimension table.
    
    Args:
        redshift_conn_id (str): The Redshift Airflow Conn Id
        aws_credentials_id (str): The SQL code to execute
        dim_table (str): the name of the dim table to insert into
        select_sql (str): the SQL query that runs the insert
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 dim_table = "",
                 select_sql = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.dim_table = dim_table
        self.select_sql = select_sql

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')

        # get aws and redshift connection credentials
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.dim_table))

        self.log.info("Running quert to insert into {self.dim_table}.")
        formatted_sql = getattr(SqlQueries, self.select_sql).format(self.dim_table)
        redshift.run(formatted_sql)