from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from operators.upload_events_to_s3 import ExportEventsToS3
from operators.upload_songs_to_s3 import ExportSongsToS3
from operators.create_or_delete_tables import CreateOrDeleteOperator
from operators.stage_to_redshift import LoadS3ToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator


default_args = {
    'owner': 'raza',
    'start_date': datetime(2020, 6, 27),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=300)
}

dag = DAG('musician_dwh',
          default_args=default_args,
          description='ETL into Musician Data Warehouse',
          schedule_interval='@daily',
          catchup=False
        )


start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

export_events_to_s3 = ExportEventsToS3(
    task_id = 'export_events_to_s3',
    dag=dag,
    bucket_name = 'mrazakazmi-data-project', # S3 bucket name that the files will be sent to
    aws_conn_id = 's3_conn', # connection to s3, created in the airflow UI.
    region = 'us-west-2',
    bucket_key = 'ticketmaster-events',
    api_key = 'N2s6VWhUUmuzxFsNwBHWGDFb5F7qyWVf'
    )

export_songs_to_s3 = ExportSongsToS3(
    task_id = 'export_songs_to_s3',
    dag=dag,
    bucket_name = 'mrazakazmi-data-project', # S3 bucket name that the files will be sent to
    aws_conn_id = 's3_conn', # connection to s3, created in the airflow UI.
    region = 'us-west-2',
    bucket_key = 'lastfm_songs',
    api_key  = '9d9df06eddd735d3d3ad36c5d55b0d09'
    )

delete_staging_tables_if_exists = CreateOrDeleteOperator(
    task_id='delete_staging_tables_if_exists',
    dag=dag,
    create_or_delete = 'delete', 
    staging_or_dwh = 'staging',
    redshift_conn_id = 'redshift_conn_id', 
)

create_staging_tables = CreateOrDeleteOperator(
    task_id='create_staging_tables',
    dag=dag,
    create_or_delete = 'create', 
    staging_or_dwh = 'staging',
    redshift_conn_id = 'redshift_conn_id', 
)

stage_events_from_s3_to_redshift = LoadS3ToRedshiftOperator(
        task_id='stage_events_from_s3_to_redshift',
        aws_credentials_id='aws_credentials_id',
        redshift_conn_id='redshift_conn_id',
        table='public.events_stage',
        s3_bucket='mrazakazmi-data-project',
        s3_key='events',
        dag=dag,
        region="us-west-2"

)

stage_songs_from_s3_to_redshift = LoadS3ToRedshiftOperator(
        task_id='stage_songs_from_s3_to_redshift',
        aws_credentials_id='aws_credentials_id',
        redshift_conn_id='redshift_conn_id',
        table='public.songs_stage',
        s3_bucket='mrazakazmi-data-project',
        s3_key='songs',
        dag=dag,
        region="us-west-2"
)

delete_dwh_tables_if_exists = CreateOrDeleteOperator(
    task_id='delete_dwh_tables_if_exists',
    dag=dag,
    create_or_delete = 'delete', 
    staging_or_dwh = 'dwh',
    redshift_conn_id = 'redshift_conn_id', 
)

create_dwh_tables = CreateOrDeleteOperator(
    task_id='create_dwh_tables',
    dag=dag,
    create_or_delete = 'create', 
    staging_or_dwh = 'dwh',
    redshift_conn_id = 'redshift_conn_id', 
)

load_artists_table = LoadFactOperator(
        task_id='load_artists_fact_table',
        redshift_conn_id="redshift_conn_id",
        table='artists',
        append=True,
        dag=dag
)

load_concerts_table = LoadDimensionOperator(
        task_id='load_concerts_dimension_table',
        redshift_conn_id="redshift_conn_id",
        table='concerts',
        append=True,
        dag=dag        
)

load_songs_table = LoadDimensionOperator(
        task_id='load_songs_dimension_table',
        redshift_conn_id="redshift_conn_id",
        table='songs',
        append=True,
        dag=dag        
)

check_data_quality = DataQualityOperator(
    task_id='check_data_quality',
    dag=dag,
    redshift_conn_id='redshift_conn_id',
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM public.songs_dwh WHERE artist is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM public.artists_dwh WHERE artists is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM public.concerts_dwh WHERE artist is null", 'expected_result': 0},
    ]
)

end_operator = DummyOperator(task_id='stop_execution', dag=dag)

start_operator>>export_events_to_s3
start_operator>>export_songs_to_s3

export_events_to_s3>>delete_staging_tables_if_exists
export_songs_to_s3>>delete_staging_tables_if_exists
delete_staging_tables_if_exists>>create_staging_tables

create_staging_tables>>stage_events_from_s3_to_redshift
create_staging_tables>>stage_songs_from_s3_to_redshift

stage_events_from_s3_to_redshift >>delete_dwh_tables_if_exists
stage_songs_from_s3_to_redshift >> delete_dwh_tables_if_exists

delete_dwh_tables_if_exists>>create_dwh_tables
create_dwh_tables>>load_artists_table
load_artists_table>>load_concerts_table
load_artists_table>>load_songs_table

load_concerts_table>>check_data_quality
load_songs_table>>check_data_quality

check_data_quality>>end_operator