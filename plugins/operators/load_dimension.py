from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
           TRUNCATE {};
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
                 append_only = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id,
        self.table = table,
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        
        redshift  = PostgresHook(postgres_conn_id = self.redshift_conn_id)
            
        if not self.append_only:           
            self.log.info("Truncating table {}".format(self.table))
            redshift.run(LoadDimensionOperator.truncate_sql.format(table=self.table))
                          
        self.log,info("Insert data into table {}".format(self.table))
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql
        )
        redshift.run(formatted_sql)                  
                          
