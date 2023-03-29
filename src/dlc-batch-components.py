import boto3
import argparse
import time
import sys

batch = boto3.client(
    service_name='batch',
    region_name='eu-west-3',
    endpoint_url='https://batch.eu-west-3.amazonaws.com/')

events = boto3.client(
    service_name='events',
    region_name='eu-west-3',
    endpoint_url='https://events.eu-west-3.amazonaws.com/')

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument("--compute-environment", help="name of the compute environment", type=str, default='dlc-gpu')
parser.add_argument("--subnets", help="comma delimited list of subnets", required=True, type=str)
parser.add_argument("--security-groups", help="comma delimited list of security group ids", required=True, type=str)
parser.add_argument("--instance-role", help="instance role", required=True, type=str)
parser.add_argument("--service-role", help="service role", required=True, type=str)
parser.add_argument("--event-role", help="EventBridge rule role", required=True, type=str)
parser.add_argument("--src-s3-bucket", help="S3 bucket hosting the DLC project", required=True, type=str)
parser.add_argument("--dlc-image", help="Container image in ECR, and that implements the DLC task", required=True, type=str)
parser.add_argument("--eventbridge-rule-name", help="name of the eventBridge rule", type=str, default='dlc-batch-trigger-rule')
args = parser.parse_args()

spin = ['-', '/', '|', '\\', '-', '/', '|', '\\']

def create_compute_environment(computeEnvironmentName, instanceType, unitVCpus, serviceRole, instanceRole,
                               subnets, securityGroups):
    response = batch.create_compute_environment(
        computeEnvironmentName=computeEnvironmentName,
        type='MANAGED',
        state='ENABLED',
        serviceRole=serviceRole,
        computeResources={
            'type': 'EC2',
            'minvCpus': unitVCpus * 1,
            'maxvCpus': unitVCpus * 2,
            'desiredvCpus': unitVCpus * 1,
            'instanceTypes': [instanceType],
            'subnets': subnets,
            'securityGroupIds': securityGroups,
            'instanceRole': instanceRole
        }
    )

    spinner = 0
    while True:
        describe = batch.describe_compute_environments(computeEnvironments=[computeEnvironmentName])
        computeEnvironment = describe['computeEnvironments'][0]
        status = computeEnvironment['status']
        if status == 'VALID':
            print('\rSuccessfully created compute environment %s' % (computeEnvironmentName))
            break
        elif status == 'INVALID':
            reason = computeEnvironment['statusReason']
            raise Exception('Failed to create compute environment: %s' % (reason))
        sys.stdout.flush()
        spinner += 1
        time.sleep(1)

    return response


def create_job_queue(computeEnvironmentName):
    jobQueueName = computeEnvironmentName + '_dlc_queue'
    response = batch.create_job_queue(jobQueueName=jobQueueName,
                                      priority=0,
                                      computeEnvironmentOrder=[
                                          {
                                              'order': 0,
                                              'computeEnvironment': computeEnvironmentName
                                          }
                                      ])

    spinner = 0
    while True:
        describe = batch.describe_job_queues(jobQueues=[jobQueueName])
        jobQueue = describe['jobQueues'][0]
        status = jobQueue['status']
        if status == 'VALID':
            print('\rSuccessfully created job queue %s' % (jobQueueName))
            break
        elif status == 'INVALID':
            reason = jobQueue['statusReason']
            raise Exception('Failed to create job queue: %s' % reason)
        sys.stdout.flush()
        spinner += 1
        time.sleep(1)

    return response


def register_job_definition(jobDefName, tgtS3Bucket, dlc_image, unitVCpus, unitMemory):
    response = batch.register_job_definition(jobDefinitionName=jobDefName,
                                             type='container',
                                             status='ACTIVE',
                                             containerProperties={
                                                 'image': dlc_image,
                                                 'command': [],
                                                 'privileged': True,
                                                 'environment': [
                                                        {
                                                                'name': 'OUTPUT_PATH',
                                                                'value': 'testDLC/output-test/'
                                                        },
                                                        {
                                                                'name': 'TGT_S3_BUCKET',
                                                                'value': tgtS3Bucket
                                                        },
                                                        {
                                                                'name': 'DLC_PROJECT_PATH',
                                                                'value': 'testDLC/dossier.zip'
                                                        }
                                                 ],
                                                'resourceRequirements': [
                                                      {
                                                        'value': unitVCpus,
                                                        'type': 'VCPU'
                                                      },
                                                      {
                                                        'value': unitMemory,
                                                        'type': 'MEMORY'
                                                      },
                                                      {
                                                        'value': '1',
                                                        'type': 'GPU'
                                                      }
                                                ],
                                                'platformCapabilities': [
                                                        'EC2'
                                                ],
                                                'containerOrchestrationType': 'ECS'
                                             })
    return response

def create_event_bridge_rule(tgtS3Bucket, eventRuleRole):
    eventPattern = '{"source": ["aws.s3"],"detail-type": ["Object Created"],"detail": { "bucket": { "name": ["' + tgtS3Bucket + '"] }, "object": { "key": [{ "prefix": "testDLC/dossier.zip" }] } }}'
    response = events.put_rule(
        Name=eventBridgeRuleName,
        EventPattern=eventPattern,
        State='ENABLED',
        Description='Event Bridge rule that triggers DLC training job',
        RoleArn=eventRuleRole
    )
    
    return response

def add_tgt_to_event_rule(eventRuleName, jobName, jobDefinitionArn):
    response = events.put_targets(
        Rule=eventRuleName,
        Targets=[
            {
                'BatchParameters': {
                    'JobDefinition': jobDefinitionArn,
                    'JobName': jobName
                }
            }
        ]
    )

    return response

def main():
    computeEnvironmentName = args.compute_environment
    serviceRole = args.service_role
    instanceRole = args.instance_role
    eventRuleRole = args.event_role
    subnets = args.subnets.split(",")
    securityGroups = args.security_groups.split(",")
    tgtS3Bucket = args.src_s3_bucket
    dlc_image = args.dlc_image
    eventBridgeRuleName = args.eventbridge_rule_name
    
    # vcpus and memory in a g4dn.xlarge
    unitVCpus = 4
    unitMemory = 16000

    create_compute_environment(computeEnvironmentName=computeEnvironmentName,
                               instanceType='g4dn.xlarge',
                               unitVCpus=4,
                               serviceRole=serviceRole,
                               instanceRole=instanceRole,
                               subnets=subnets,
                               securityGroups=securityGroups)

    create_job_queue(computeEnvironmentName)

    jobDefinition = register_job_definition('dlc-gpu-platform', tgtS3Bucket, dlc_image, unitVCpus, unitMemory)
    jobDefinitionArn = jobDefinition['jobDefinitionArn']

    create_event_bridge_rule(eventBridgeRuleName, tgtS3Bucket, eventRuleRole)
    add_tgt_to_event_rule(eventBridgeRuleName, jobDefName, jobDefinitionArn)


if __name__ == "__main__":
    main()
