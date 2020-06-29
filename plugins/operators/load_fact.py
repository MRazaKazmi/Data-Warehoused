from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):
        """
        The custom operator to transfer the data from staging to fact table in Redshift.
        It uses the SqlQueries statement in the helpers function.
        Args:
                redshift_conn_id: The configuration to connect to Redshift
                table: The name of the table to run the ingestion
                append: If append is True, it will append the data from staging to dimension table. 
                        Else, it will clear all the data, before putting data into dimension table.
        """
        template_fields = ("table",)
        ui_color = '#F98866'

        @apply_defaults
        def __init__(self,
                        redshift_conn_id="",
                        table="",
                        append=True,
                        *args, **kwargs):
                
                super(LoadFactOperator, self).__init__(*args, **kwargs)
                self.redshift_conn_id = redshift_conn_id
                self.table = table
                self.append = append
        
        def execute(self, context):
                redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

                if (self.append == False):
                        self.log.info(f"Clearing data from Redshift {self.table} table")
                        redshift.run("DELETE FROM {}".format(self.table))
                
                self.log.info(f"Start loading the { self.table } fact table")
                if (self.table == "artists"):
                        redshift.run(SqlQueries.artists_insert)
                else:
                        self.log("No table found")