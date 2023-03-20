import boto3

print('Loading function')

s3 = boto3.client('s3')
glue = boto3.client('glue')
ssm=boto3.client('ssm')

def lambda_handler(event, context):

    envParam = ssm.get_parameter(Name='/env', WithDecryption=True)
    env = envParam['Parameter']['Value']


    glueParam = ssm.get_parameter(Name='/'+env +'/myapp/gluejob', WithDecryption=True)
    gluename = glueParam['Parameter']['Value']

    bucket_name =event['Records'][0]['s3']['bucket']['name']
    event_key = event['Records'][0]['s3']['object']['key']
    print(" lambda triggered for bucket: "+bucket_name+" , event_key: "+event_key)
    glueJobName= gluename
    runId = glue.start_job_run(JobName= glueJobName,Arguments={})
    print(runId)
    status = glue.get_job_run(JobName= glueJobName, RunId=runId['JobRunId'])
    print(status)
    return bucket_name, event_key
    
    



    

    