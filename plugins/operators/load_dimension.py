from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):
    """
    The custom operator to transfer the data from staging to dimension table in Redshift.
    It uses the SqlQueries statement in the helpers function.
    Args:
        redshift_conn_id: The configuration to connect to Redshift
        table: The name of the table to run the ingestion
        append: If append is True, it will append the data from staging to dimension table. 
                Else, it will clear all the data, before putting data into dimension table.
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 append=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append = append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        if (self.append == False):
            self.log.info(f"Clearing data from Redshift {self.table} table")
            redshift.run("DELETE FROM {}".format(self.table))
                
        self.log.info(f"Start loading the { self.table } fact table")
        if (self.table == "concerts"):
            redshift.run(SqlQueries.concerts_insert)
        elif(self.table == "songs"):
            redshift.run(SqlQueries.songs_insert)
        else:
            self.log("No table is found.")