# Building an Analytical Data View of Music Data for Analytics.

**Step 1: Scope and Exploration**

The goal of this data engineering project is to build a data pipeline using Apache Airflow which collects data from two data sources via API: events data from the Ticketmaster API and songs data from the Last.fm API. The extracted data is stored into AWS S3, after which it is staged in Redshift and loaded into a star schema data warehouse model, which is optimized for analytical queries. The data extracted from the API is explored and assessed in a notebook environment before the pipleline is built. 

**Step 2: Data Model**

The data is modeled as in the Entity-Relationship Diagram shown below:

![alt text](https://github.com/MRazaKazmi/airflow-datapipeline-project/blob/master/images/data_model.png)

The data model contains one fact table which contains information about artists such as artist name and listeners. Surrounding this fact table are two dimension tables - one containg information about songs such as listners and another containing information about concerts such as date and location. Some of the queries which can be run on this data model include finding the most popular song of an artist or the most popular concert location. 

**Data Dictionary**

**songs_dwh**

| Column  | Type |Description |
| ------- | ------------- |------------- |
| song    | varchar  | the name of the song |
| song_playcount  | integer  | the number of times the song has been played |
| song_listeners  | integer  | the number of listeners of the song |
| artist  | varchar  | the artist associated with the song |


**artists_dwh**

| Column  | Type |Description |
| ------- | ------------- |------------- |
| artist    | varchar  | the name of the artist |
| artist_listeners  | integer  | the number of listeners of the artist |


**concderts_dwh**

| Column  | Type |Description |
| ------- | ------------- |------------- |
| name    | varchar  | the name of the concert |
| location  | varchar  | the concert's location |
| start_date  | date  | the concert's date |
| artist    | varchar  | the name of the artist performing at the concert|



**Step 3: Data Pipeline**

The data pipeline is visualized below:

![alt text](https://github.com/MRazaKazmi/airflow-datapipeline-project/blob/master/images/data_pipeline.png)

1.	The first step in the pipeline is to save data fetched from the API into S3 buckets. It is generally recommended to do so before staging into Redshift for transformation into the star schema data model in Redshift. 
2.	Data is then staged in Redshift
3.	It is then transformed into two dimensional tables and one fact table
4.	Finally, data quality is checked by ensuring that the tables do not contain null values. 



### Technologies Leveraged:

Airflow is used as the pipeline orchestrator as it is relatively straightforward to build data pipeline using it. One can easily breakdown the pipeline into tasks which are related to each other with dependencies. This also allows parallelization of task execution as independent tasks can be run at the same time. The Airflow UI is very user friendly and one can easily inspect the pipeline using the various views provided such as the graph and tree view to diagnose any errors. 

Redshift is used as the cloud data warehouse because it is highly scalable. With Redshift as the technology of choice for the cloud warehouse, S3 is used as the data’s cloud storage. Both the cloud services are provided by AWS and Redshift has built in support for extracting data from S3. 

Star-Schema Data Model is used because the dataset is built for analytics and read-heavy analytics workloads are optimized with the Star-Schema Data Model.





### Instructions for Running the Pipeline:

1.	Create API KEYS for the two data sources and keep them secret
2.	Create Redshift cluster, by running the create_cluster.py script in the aws-redshift folder, ensuring that the cluster is in the same region as the S3 bucket
3.	Provision the local environment using Docker, whereby the puckel/docker-airflow image is used. 
4.	Inside the root folder, run the following command:

docker-compose -f docker-compose-LocalExecutor.yml up -d

5.	Add the following necessary connections in the Airflow UI:
•	aws_credentials_id: connection type "Amazon Web Services", enter your AWS admin user access Key ID in the login fields and secret Access Key in the password field.
•	s3_conn: connection type "S3" and in the extras field paste your AWS admin user access Key ID and Secret Access Key: {"aws_access_key_id":"XXX", "aws_secret_access_key": "XXX"}
•	redshift: connection type “Postgres”, the host is the endpoint, schema is the database name followed by the database user and password generated when creating the cluster, and finally add the port number which would be 5439.
6.	Delete Redshift cluster running the delete_cluster.py script in the same folder as the create cluster script. 

### Future Cases:

1. If the data from the sources increase by 100x it would not have an impact on the data pipeline as Redshift is capable of scaling to incorporate massive datasets for read-heavy workloads. If however we had write-heavy workloads we would use a NoSQL database for scaling to incorporate massive datasets. For a read-heavy workload in Redshift, data may be partitioned by date. 
2. If the pipelines were run daily it would have no significant impact on the data pipeline because it is already scheduled to run daily; the only change which needs to be made is to specify 7am to the scheduler. 
3. If the data warehouse was accessed by 100+ people it would have no major impact on the data pipeline because Redshift by design can handle it due to its auto-scaling capability to increase its clusters and meet concurrency needs. 
