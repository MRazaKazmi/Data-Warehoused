"""
The purpose of this custom operator is create or delete tables in redshift automatically.
The tables to be created or deleted are found in the create_table_queries and delete_table_queries variables in sql_queries.py
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers.sql_queries import SqlQueries

class CreateOrDeleteOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 create_or_delete = "",
                 staging_or_dwh = "",
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(CreateOrDeleteOperator, self).__init__(*args, **kwargs)
        self.create_or_delete = create_or_delete
        self.redshift_conn_id = redshift_conn_id
        self.staging_or_dwh = staging_or_dwh

    def execute(self, context):
        # connects to redshift using the connection created in the airflow UI
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Connection to redshift is successfull.')
        
        # deletes tables mentioned in the delete_table_queries variable in sql_queries.py
        if self.create_or_delete == 'delete':
            if self.staging_or_dwh == 'staging':
                self.log.info('Dropping staging tables...')
                for query in SqlQueries.delete_staging_table_queries:
                    redshift.run(query)
                    self.log.info('Staging tables dropped.')
            else:
                self.log.info('Dropping dwh tables...')
                for query in SqlQueries.delete_dwh_table_queries:
                    redshift.run(query)
                    self.log.info('Dwh tables dropped.') 

            
        # creates tables mentioned in the create_table_queries variable in sql_queries.py
        if self.create_or_delete == 'create':
            if self.staging_or_dwh == 'staging':
                self.log.info('Creating staging tables...')
                for query in SqlQueries.create_staging_table_queries:
                    redshift.run(query)
                    self.log.info('Staging tables created.')
            else:
                self.log.info('Creating dwh tables...')
                for query in SqlQueries.create_dwh_table_queries:
                    redshift.run(query)
                    self.log.info('Dwh tables created.')
                