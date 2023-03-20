import boto3
import shutil

print('Loading function')

s3 = boto3.client('s3')
glue = boto3.client('glue')
ssm = boto3.client('ssm')

import shutil

def lambda_handler(event, context):

    envParam = ssm.get_parameter(Name='/env', WithDecryption=True)
    env = envParam['Parameter']['Value']
    moduleParam = ssm.get_parameter(Name='/module', WithDecryption=True)
    module_name = moduleParam['Parameter']['Value']

    # getting bucket name using parameter store
    bucketParam = ssm.get_parameter(
        Name='/'+env+'/myapp/scriptbucket', WithDecryption=True)
    target_name = bucketParam['Parameter']['Value']

    # upload glue script to the s3 bucket
    with open("./glue_script.py", "rb") as file:
        s3.upload_fileobj(file, target_name, env +
                          '/scripts/{}'.format("glue_script.py"))

        # upload zip sample code to s3 bucket
    
    with open("./"+module_name+".zip", "rb") as zip:
        s3.upload_fileobj(zip, target_name, env +
                          '/package/{}'.format(module_name+".zip"))

    glueParam = ssm.get_parameter(
        Name='/'+env + '/myapp/gluejob', WithDecryption=True)
    gluename = glueParam['Parameter']['Value']

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    event_key = event['Records'][0]['s3']['object']['key']
    print(" lambda triggered for bucket: " +
          bucket_name+" , event_key: "+event_key)
    glueJobName = gluename
    runId = glue.start_job_run(JobName=glueJobName, Arguments={})
    print(runId)
    status = glue.get_job_run(JobName=glueJobName, RunId=runId['JobRunId'])
    print(status)
    return bucket_name, event_key
