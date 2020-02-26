
# load parameters
from botocore.exceptions import ClientError
from tabulate import tabulate
from time import time
import pandas as pd
import configparser 
import psycopg2 
import boto3
import time
import json
import sys

# load parameters
def createCluster():
    config = configparser.ConfigParser() # creer le fichier de configuaration en memoire
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")

    CIDRIP                 = config.get("EC2", "CIDRIP")


    param = pd.DataFrame({"Param":
                    ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                "Value":
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                })
    print('    ---> Parameters <---    ')
    print(tabulate(param, headers='keys', tablefmt='rst', showindex=False))

    # get client and ressources AWS
    ec2 = boto3.resource('ec2', 
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name="us-west-2")

    s3 = boto3.resource('s3', 
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name="us-west-2")

    iam = boto3.client('iam',
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET,
                    region_name="us-west-2"
                    )

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                        )
  
    # 
    print('\n')
    print('   --->> Check an Iam Role <<---   ') 
    createRole(iam, DWH_IAM_ROLE_NAME)
    roleArn=attachPolicy(iam, DWH_IAM_ROLE_NAME)


    print('    --->> Check if cluster exists <<---    ')
    DWH_ENDPOINT = clusterTest(redshift, DWH_CLUSTER_IDENTIFIER)

    if DWH_ENDPOINT != '-2' and DWH_ENDPOINT != '-1': 
        print('Cluster running')
        print('DWH_ENDPOINT :: {}'.format(DWH_ENDPOINT))

    if DWH_ENDPOINT == '-1':
        DWH_ENDPOINT = lunchCluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES,DWH_DB, DWH_CLUSTER_IDENTIFIER,DWH_DB_USER,DWH_DB_PASSWORD)

    while DWH_ENDPOINT == '-2':        
        print('Waiting for cluster to be ready ...')
        DWH_ENDPOINT = clusterTest(redshift, DWH_CLUSTER_IDENTIFIER)
        if DWH_ENDPOINT == '-2':
            time.sleep(10)
        else:
            print("Cluster Created and running")

    print('\n')        
    print('    --->> Connect to the port <<---    ')
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]       
    portEc2(ec2, myClusterProps, DWH_PORT)

    print('    --->> Connect to the database <<---    ')
    conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
    print(conn_string)
    if conn_string:
        print('Connected to the database {} as {}.'.format(DWH_DB, DWH_DB_USER))
        print('\n')
# functions 

def createRole(iam, DWH_IAM_ROLE_NAME):
    '''
    Create IAM role and attaching policy, to allow Redshift clusters to call AWS swevices
    ''' 
    try:

        print('Creating a new Iam Role...')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description= "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})        
        )        
    except Exception as e:
        print(e)
    
    return()

def attachPolicy(iam, DWH_IAM_ROLE_NAME):
    print('\n')
    print('    --->> Attaching Policy <<---    ')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print("Get the IAM role ARN...")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print(roleArn)
    print("Done Create Role")
    print('\n')
    return(roleArn)


def lunchCluster(redshift,roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES,DWH_DB, DWH_CLUSTER_IDENTIFIER,DWH_DB_USER,DWH_DB_PASSWORD ):
    '''
    Create the cluster with the client redshift 
    '''
    try:
        response = redshift.create_cluster(        
            # parameters for hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # parameters for identifiers & credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,       

            # parameter for role (to allow s3 access)
            IamRoles=[roleArn]

        )
        print('\n')
        print('    --->> Create cluster beginning... <<---')
    except Exception as e:
        print(e)
    
    return('-2')


def prettyRedshiftProps(props):
    '''
    display the Cluster Status
    '''
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    props=pd.DataFrame(data=x, columns=["Key", "Value"])
    return(print(tabulate(props, headers='keys', tablefmt='rst', showindex=False)))

def clusterTest(redshift, DWH_CLUSTER_IDENTIFIER):
    clusterCreate= '-2'
    if clusterCreate != '-1':
        try:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            if myClusterProps['ClusterStatus'] == 'available':
                DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
                DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
                prettyRedshiftProps(myClusterProps)
                print('\n')      
                return(DWH_ENDPOINT)
            elif myClusterProps['ClusterStatus'] == 'creating':
                return('-2')
            print('Cluster Status : {}'.format(myClusterProps['ClusterStatus']))
        except Exception as e:
            print('Cluster is done, so here we go!')
            print(e)        
            return('-1')
           
    if clusterCreate == '-2':
        try:
            DWH_ENDPOINT = None
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
            DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
                 
            clusterCreate = '-1'
        except Exception as e:
            clusterCreate = '-2'
            print('processing....')
        print('    {}'.format(DWH_ENDPOINT))
        return(DWH_ENDPOINT)

def portEc2(ec2, myClusterProps, DWH_PORT):
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp=CIDRIP,
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print()
    return(defaultSg)





if __name__ == "__main__":
    
    createCluster()

    # print('\n\nAll good, commencing ETL.')
    # Smain(create_table_queries, drop_table_queries)
    # etl_main(copy_table_queries, insert_table_queries)