from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
           INSERT 
           INTO
           {}
           {}
       """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = "",
                 table = "",
                 sql = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id,
        self.table = table,
        self.sql = sql
        
    def execute(self, context):
        
        redshift  = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Inserting data from staging tables into fact table songplays")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql
        )
        redshift.run(formatted_sql)
