from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import re


class LoadS3ToRedshiftOperator(BaseOperator):
    """
    The custom operator will clear all the data in the staging table. 
    Then, copy the data from S3 bucket and load them into the staging tables.
    
    Args:
        aws_credentials_id: The access and secret keys to access AWS resources (i.e. S3 bucket)
        redshift_conn_id: The configuration to connect to Redshift
        table: The name of the staging table to store the data
        s3_bucket: The name of bucket
        s3_key: The folder and filename
        region: The region used in the AWS environment. Default is us-west-2
    """
    template_fields = ("s3_key",)
    ui_color = '#358140'   
    copy_sql = """    
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        delimiter ';' 
        DATEFORMAT 'YYYY-MM-DD' 
        IGNOREHEADER AS 1
    """

    @apply_defaults
    def __init__(self,
                    aws_credentials_id="",
                    redshift_conn_id="",
                    table="",
                    s3_bucket="",
                    s3_key="",
                    region="",
                    *args, **kwargs):
        
        super(LoadS3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info(f"Clearing data from destination Redshift {self.table} table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f"Copying data from s3 to Redshift {self.table} table")
        rendered_key = self.s3_key.format(**context)
        rendered_key_no_dashes = re.sub(r'-', '', rendered_key)
        self.log.info("Rendered Key no dashes {}".format(rendered_key_no_dashes))
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key_no_dashes)
        

        formatted_sql = LoadS3ToRedshiftOperator.copy_sql.format(
            self.table ,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region
        )
    

        redshift.run(formatted_sql)