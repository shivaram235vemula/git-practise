import json
import boto3
import os
import botocore
import requests
import random
import utilitymodules as um
import time
from datetime import date, timedelta

TEMPLATE = '''This alert contains information about EKS Node group AMI upgrade status

AWS Account: {}
Result: 
{}
'''

def get_tag_values(credentials, account_id):
    assignment_group_tag_key = 'remediation-group'
    env_tag_key = 'env-type'
    assignment_group = ''
    env_type = ''
    
    session = boto3.session.Session()
    dynamodb = session.resource(
        service_name='dynamodb',
        region_name='us-east-1',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )

    table = dynamodb.Table('AccountDetails')
    try:
        tagdict = table.get_item(Key={'account-id': account_id})
        assignment_group = tagdict['Item'][assignment_group_tag_key]
        env_type = tagdict['Item'][env_tag_key]

    except Exception as e:
        print(f"Error fetching tagname value from DynamoDB for account id {account_id}: {e}")
    finally:
        return assignment_group, env_type

def publish_msg(subject, message):
    is_status_notified = True
    region = os.environ['AWS_REGION']
    topic_name = os.environ['SNS_TOPIC_NAME']
    SNS_TOPIC_ARN = f'arn:aws:sns:{region}:848721808596:{topic_name}'
    try:
        sns = boto3.client('sns')
        sns.publish(TopicArn=SNS_TOPIC_ARN, Message=message, Subject=subject, MessageStructure='string')
    except Exception as e:
        is_status_notified = False
        print(f"Exception is: {e}")
    finally:
        print(f"Could not publish message to SNS topic '{SNS_TOPIC_ARN}'")
        return is_status_notified

def submit_snow_request(params):
    snow_req_url = os.environ['SNOW_API_URL']
    service_account = os.environ['SERVICE_ACCOUNT']

    nodeGroupName = params['nodeGroupName']
    master_account = '848721808596'
    master_role = 'OrganizationsReadAccessRole'
    credentials = um.assume_role(master_account, master_role)
    assignment_group, env_type = get_tag_values(credentials, params['accountId'])
    
    if not assignment_group:
        assignment_group = 'Cloud Support Platform'

    print(assignment_group)
    print(env_type)

    short_description = f"EKS Nodegroup AMI Update failed for the Node group: {nodeGroupName}"
    description = (f"EKS Nodegroup AMI Update failed. Below are the details:\n\n"
                   f"Event: EKS Nodegroup AMI Update\n"
                   f"Account Id: {params['accountId']}\n"
                   f"Account Name: {params['accountName']}\n"
                   f"Region/AZ: {params['region']}\n"
                   f"ClusterName: {params['clusterName']}\n"
                   f"NodeGroupName: {nodeGroupName}\n"
                   f"Status: {params['status']}\n"
                   f"ErrorMessage: {params['errorMessage']}")

    try:
        complete_date = date.today() + timedelta(days=7)
        complete_date_string = complete_date.strftime('%Y-%m-%d %H:%M:%S')
        
        params = {
            'service_account': service_account,
            'item_sys_id': '7d1b47f4b57b62de9f5794ec034bcbe70',
            'snow_api_url': snow_req_url,
            'assigned_group': assignment_group,
            'opened_by': 'AWS Compliance Alerts',
            'completed_on': complete_date_string,
            'requested_for': 'AWS Compliance Alerts',
            'sysparm_quantity': '1',
            'action_text': description
        }
        
        request_number = ''
        request_sys_id = ''
        rtm_id = ''
        is_snow_req_created, request_number, request_sys_id, rtm_id = um.create_request_item(params)
        
        if is_snow_req_created:
            print(request_number)
            snow_task_url = os.environ['SNOW_TASK_API_URL']
            sc_task_id, sys_id = um.get_sc_task(service_account, snow_task_url, rtm_id)
            if sc_task_id:
                priority = '2' if env_type in ['prd'] else '3'
                payload = {
                    'short_description': short_description,
                    'priority': priority,
                    'assignment_group': assignment_group
                }
                um.update_sc_task(snow_req_url, sys_id, payload)

        return is_snow_req_created, request_number, request_sys_id

    except Exception as e:
        print(f"Error submitting SNOW request: {e}")
        raise e

def load_to_s3(message):
    bucket_name = 'organization-repo-logs'
    account_id = message['accountId']
    region = message['region']
    clusterName = message['clusterName']
    nodeGroupName = message['nodeGroupName']
    json_object = json.dumps(message)
    
    random_number = random.randint(1000000000, 9999999999)
    object_key = f'sm-{account_id}/{region}/{clusterName}/{nodeGroupName}-{random_number}'
    
    try:
        print('Load to S3')
        client = boto3.client('s3', region_name='us-west-1')
        client.put_object(Body=json_object, Bucket=bucket_name, Key=object_key, ACL='bucket-owner-full-control')
    except botocore.exceptions.ClientError as e:
        print(f"Error uploading object to S3: {e}")
        raise e

def get_cluster_tags(clustername, region):
    eks_client = boto3.client('eks', region_name=region)
    response = eks_client.describe_cluster(name=clustername)
    return response['cluster']['tags']

def process_message(account_id, account, nodegroups):
    print('Process message')
    final_result = ''
    region = os.environ['AWS_REGION']

    try:
        for nodegroup in nodegroups:
            log_dict = {
                'category': 'EKS-AMT-Upgrade',
                'accountId': account_id,
                'accountName': account,
                'region': nodegroup['region'],
                'clusterName': nodegroup['ClusterName'],
                'nodeGroupName': nodegroup['NodeGroupName']
            }

            tags = get_cluster_tags(nodegroup['ClusterName'], nodegroup['region'])
            if tags:
                for key, value in tags.items():
                    log_dict[key] = value.upper()

            if 'id' in nodegroup:
                eks_client = boto3.client('eks', region_name=nodegroup['region'])
                log_dict['updateId'] = nodegroup['id']
                response = eks_client.describe_update(name=nodegroup['ClusterName'], updateId=nodegroup['id'])
                log_dict['status'] = response['update']['status']

                final_result += f"\nClusterName: {nodegroup['ClusterName']}\nNodeGroupName: {nodegroup['NodeGroupName']}"
                if response['update']['status'] == 'Failed':
                    for error in response['update']['errors']:
                        final_result += f"\nErrorCode: {error['errorCode']}\nErrorMessage: {error['errorMessage']}"
                        log_dict['errorCode'] = error['errorCode']
                        log_dict['errorMessage'] = error['errorMessage']
            
            is_snow_req_created, request_number, req_sys_id = submit_snow_request(log_dict)
            if is_snow_req_created:
                log_dict['snowRequestNumber'] = request_number
                log_dict['snowSysId'] = req_sys_id

            load_to_s3(log_dict)

        subject = f'EKSPatchingStatus: {account}'
        message = TEMPLATE.format(account, final_result)
        if final_result:
            is_status_notified = publish_msg(subject, message)

    except Exception as ex:
        print(f"Unable to process AMI patching notification: {ex}")
    
def check_execution_status(nodegroups):
    is_execution_completed = True

    try:
        for nodegroup in nodegroups:
            if 'id' in nodegroup:
                eks_client = boto3.client('eks', region_name=nodegroup['region'])
                response = eks_client.describe_update(name=nodegroup['ClusterName'], updateId=nodegroup['id'])

                if response['update']['status'] == 'InProgress':
                    is_execution_completed = False
                    break
    except Exception as ex:
        print(f"Unable to check execution status: {ex}")
    
    return is_execution_completed

def lambda_handler(event, context):
    print(event)
    index = event.get('index', 0)
    account_id = context.invoked_function_arn.split(":")[4]
    aws_acc = boto3.client('iam').list_account_aliases()['AccountAliases'][0]

    if 'output' in event:
        nodegroups = event['output']
        is_execution_completed = event.get('is_execution_completed', False)

        if not is_execution_completed:
            is_execution_completed = check_execution_status(nodegroups)

        if is_execution_completed:
            is_status_notified = process_message(account_id, aws_acc, nodegroups)

        event['is_execution_completed'] = is_execution_completed
        event['is_status_notified'] = is_status_notified

    event['index'] = index + 1
    return event
