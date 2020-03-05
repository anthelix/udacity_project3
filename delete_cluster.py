from botocore.exceptions import ClientError
from tabulate import tabulate
from time import time
import pandas as pd
import configparser 
import boto3
import time
import sys


def endOfCluster():
    '''
    Get parameters, check if cluster then delete it with calls to the others functions
    '''
    # load parameters
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")

    DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")


    param = pd.DataFrame({"Param":
                    ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_IAM_ROLE_NAME"],
                "Value":
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_IAM_ROLE_NAME]
                })
    print('\n')            
    print('    ---> Parameters <---    ')
    print(tabulate(param, headers='keys', tablefmt='rst', showindex=False))
    
    # get client and ressources AWS
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

    # check before deleting                  
    myClusterProps = getClusterId(redshift, DWH_CLUSTER_IDENTIFIER)
    print('\n')
    print('    ---> Cluster status before deleting <---    ')
    if myClusterProps[0] !='':
        try:
            prettyRedshiftProps(myClusterProps[0])
            DWH_ENDPOINT = myClusterProps[1]
            DWH_ROLE_ARN = myClusterProps[2]
            print("You have a point, Cluster exists")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ClusterNotFound':
                print("Cluster Doesn't exists")
            else:
                print ("Unexpected error: %s" % e)


    print('\n')
    # deleting the cluster
    deleteCluster(redshift, DWH_CLUSTER_IDENTIFIER)
    
    # test the cluster status
    res = testDelete(redshift, DWH_CLUSTER_IDENTIFIER)
    while(res==False):
            res = testDelete(redshift, DWH_CLUSTER_IDENTIFIER)
            time.sleep(20)
    
    # deleting the iam role
    deleteRole(iam, DWH_IAM_ROLE_NAME)


# Test the cluster status or if exists
def testDelete(redshift, DWH_CLUSTER_IDENTIFIER):
    '''
    Test the cluster status or if exists
    '''
    clusterDelete = False
    if clusterDelete != True:            
        try:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            print('Wait during {}'.format(myClusterProps['ClusterStatus']))
            if myClusterProps['ClusterStatus'] == 'available':
                DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
                DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
                prettyRedshiftProps(myClusterProps)
                print('\n')      
                return(False)
            elif myClusterProps['ClusterStatus'] == 'creating':                
                print('Wait creating befor deleting --> Cluster Status : {}'.format(myClusterProps['ClusterStatus']))
                return(False)
            elif myClusterProps['ClusterStatus'] == 'deleting':
                return(False)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ClusterNotFound':
                print('Cluster is done! ')          
                return('-1')
            else:
                print ("Unexpected error: %s" % e) 

    if clusterDelete == False:
        try:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
            DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
            return(False)
        except Exception as e:
            print('processing...')
            return(True)

# Delete the iam role
def deleteRole(iam, DWH_IAM_ROLE_NAME):
    '''
    Delete the iam role
    '''
    print("\n")
    print('IAM role is deleting...')
    try:
        iam.detach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        iam.delete_role(
            RoleName=DWH_IAM_ROLE_NAME
        )
    except Exception as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print("IamRole Doesn't exists")
        else:
            print ("Unexpected error: %s" % e)
    return()

# Delete the cluster
def deleteCluster(redshift, DWH_CLUSTER_IDENTIFIER):
    '''
    Delete the cluster
    '''
    try:
        redshift.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ClusterNotFound':
            print("Cluster Doesn't exists")
        else:
            print ("Unexpected error: %s" % e)

# Display the cluster parameters  
def prettyRedshiftProps(props):
    '''
    display the Cluster Status
    '''
    keysToShow = ["ClusterIdentifier", "ClusterStatus", "MasterUsername", "DBName"]
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    props=pd.DataFrame(data=x, columns=["Key", "Value"])
    return(print(tabulate(props, headers='keys', tablefmt='rst', showindex=False)))

# Get the cluster ID
def getClusterId(redshift, DWH_CLUSTER_IDENTIFIER):
    '''
    to get the myClustersProps, dwh_enpoint, dwh_role_arn
    '''
    myClusterProps = redshift.describe_clusters()
    if len(myClusterProps['Clusters']) == 0:
        return ('', '', '')
    else:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        return(myClusterProps, DWH_ENDPOINT, DWH_ROLE_ARN)

if __name__ == "__main__":
    
    endOfCluster()
    
