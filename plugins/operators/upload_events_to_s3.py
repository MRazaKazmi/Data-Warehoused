"""
The puprose of this custom operator is to export files from the local machine to an S3 bucket.
"""
import os
import glob
import requests
import json
import pandas as pd
import s3fs


from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ExportEventsToS3(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 bucket_name = "",
                 aws_conn_id = "",
                 region = "",
                 bucket_key = "",
                 api_key="",
                 *args, **kwargs):

        super(ExportEventsToS3, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.aws_conn_id = aws_conn_id
        self.region = region
        self.bucket_key  = bucket_key
        self.api_key = api_key
        
    def execute(self, context):
        
        # connects to S3 using the connection created in the airflow UI.
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)

        aws_hook = AwsHook('aws_credentials_id')
        credentials = aws_hook.get_credentials() 
        os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key
        AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
        AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

        params={'countryCode': 'US','classificationName': 'Musician','apikey': 'N2s6VWhUUmuzxFsNwBHWGDFb5F7qyWVf'}


        if s3.check_for_bucket(self.bucket_name):
            self.log.info('Bucket exists.')
        else:
            s3.create_bucket(self.bucket_name,self.region)
            self.log.info('Bucket created.')
        
        self.log.info('Collecting data...')
            
        self.log.info('Connecting to API to query data...')

        def ticketmaster_get(country, category):
    
            # define URL
            url = 'https://app.ticketmaster.com/discovery/v2/events.json'

            # Add API key and format to the payload

            payload={
                    'countryCode': country,
                    'classificationName': category,
                    'apikey': self.api_key
                }
            response = requests.get(url, params=payload)

            if response.status_code != 200:
                return None
            
            json_data = response.json()

            data = []
            cols = ['concert', 'artist', 'location', 'start_date']
            if '_embedded' in json_data:
                for x in json_data['_embedded']['events']:
                    name = x['name']
                    musician_name = x['_embedded']['attractions'][0]['name']
                    location = x['_embedded']['venues'][0]['name']
                    start_date = x['dates']['start']['localDate']
                    data.append([name, musician_name, location, start_date])
                    
            events_dataframe = pd.DataFrame(data, columns=cols)
                    
            return events_dataframe

        events_info = ticketmaster_get('US', 'Musician')

    
        self.log.info("Saving data to s3")

        self.log.info("Saving data to s3")

        bytes_to_write = events_info.to_csv(sep=';').encode()
        fs = s3fs.S3FileSystem(key=AWS_ACCESS_KEY_ID, secret=AWS_SECRET_ACCESS_KEY)
        with fs.open('s3://mrazakazmi-data-project/events.csv', 'wb') as f:
            f.write(bytes_to_write)

        self.log.info("Data saved")

        self.log.info("Data saved")
    


     