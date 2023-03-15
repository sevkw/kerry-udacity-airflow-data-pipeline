from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    """
	This operator load any JSON formatted files from S3 to Amazon Redshift.
	The operator creates and runs a SQL COPY statement based on the parameters provided.
	redshift_conn_id: the redshift connection configured in Airflow Connection
	aws_credentials_id: the aws credential connection configured in Airflow Connection
	table: the target table to load data in Amazon Redshift
	s3_path: the s3 bucket name to load source data
	format: the format of the data
    """
    ui_color = '#358140'

    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            {} {} 
           
        """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table="",
                 s3_bucket="",
	             s3_key="",
	             region="",
	             data_format ="",
                 jsonpath = 'auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.data_format = data_format
        self.jsonpath = jsonpath
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')

        # get aws and redshift connection credentials
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
	        self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
			self.region,
            self.data_format,
            self.jsonpath
        )
				
        redshift.run(formatted_sql)