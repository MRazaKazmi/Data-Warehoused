import boto3
import configparser
import pandas as pd
import json
from botocore.exceptions import ClientError
from time import sleep

def get_config(filename, section):
    access = configparser.ConfigParser()
    access.read_file(open(filename))
    
    return access[section].values()

def main():
    # Fetch Params
    print("1. Fetch params")
    KEY, SECRET = get_config('access.cfg', 'AWS')
    DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME = get_config('dwh.cfg', 'DWH')
    
    # Setup resources and clients
    print("2. Setup Clients")
    iam = boto3.client('iam',region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    redshift = boto3.client('redshift', region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    
    # Delete the Redshift Clusters
    print("3. Deleting Redshift Clusters")
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    while (myClusterProps['ClusterStatus'] == 'deleting'):
        print ("Redshift is {}".format(myClusterProps['ClusterStatus']))
        sleep(60)
        try:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        except Exception as e:
            print(e)
            break;
            
    print("Redshift is deleted")
    
    print("4. Clean up Resources")
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    
if __name__ == "__main__":
    main()