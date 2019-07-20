from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key","execution_date",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
        JSON '{}'
    """   

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id ="" ,
                 aws_credentials_id ="" ,
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 delimiter = ",",
                 ignore_headers = 1,
                 json_path="",
                 data_format = "",
                 execution_date = "",
                 *args, **kwargs
                      ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table,
        self.redshift_conn_id = redshift_conn_id,
        self.s3_bucket = s3_bucket,
        self.s3_key = s3_key,
        self.delimiter = delimiter,
        self.ignore_headers = ignore_headers,
        self.aws_credentials_id = aws_credentials_id,
        self.json_path = json_path
        self.data_format = data_format,
        self.execution_date = execution_date
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift  = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Clearing data from Redshift destination table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        rendered_date = self.execution_date.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter,
            self.json_path,
            self.data_format,
            year = rendered_date.year,
            month = rendered_date.month
        )
        redshift.run(formatted_sql)





