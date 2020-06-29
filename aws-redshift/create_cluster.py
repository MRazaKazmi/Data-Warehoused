import boto3
import configparser
import pandas as pd
import json
from botocore.exceptions import ClientError
from time import sleep

def prettyRedshiftProps(props):
    pd.set_option('display.max_columns', 3)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def get_config(filename, section):
    access = configparser.ConfigParser()
    access.read_file(open(filename))
    
    return access[section].values()

def create_iam(iam, DWH_IAM_ROLE_NAME):
    #1.1 Create the role, 
    try:
        print("3.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)

    #1.2 Attach Policy
    print("3.2 Attaching Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']
    
    print("3.3 Get the IAM role ARN")
    return iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

def create_redshift_cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn):
    try:
        redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)

def create_vpc(ec2, DWH_PORT, myClusterProps):
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
    
def main():
    # Fetch Params
    print("1. Fetch params")
    KEY, SECRET = get_config('dwh.cfg', 'AWS')
    DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME = get_config('dwh.cfg', 'DWH')
    
    # Setup resources and clients
    print("2. Setup Clients and resources")
    ec2 = boto3.resource('ec2', region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    iam = boto3.client('iam',region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    redshift = boto3.client('redshift', region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    
    # Create IAM role
    roleArn = create_iam(iam, DWH_IAM_ROLE_NAME)
    
    # Create Redshift Cluster
    print("4. Creating Redshift Cluster")
    create_redshift_cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn)
    
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    while (myClusterProps['ClusterStatus'] != 'available'):
        print ("Redshift is {}".format(myClusterProps['ClusterStatus']))
        sleep(30)
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        
    print("Redshift is {}".format(myClusterProps['ClusterStatus']))
    print(prettyRedshiftProps(myClusterProps))
    
    # Open incoming TCP port to access cluster endpoint
    print("5. Setup incoming TCP port...")
    create_vpc(ec2, DWH_PORT, myClusterProps)
    
    # Redshift Cluster and Endpoint
    print("DWH_ENDPOINT :: {}".format(myClusterProps['Endpoint']['Address']))
    print("DWH_ROLE_ARN :: {}".format(myClusterProps['IamRoles'][0]['IamRoleArn']))

if __name__ == "__main__":
    main()