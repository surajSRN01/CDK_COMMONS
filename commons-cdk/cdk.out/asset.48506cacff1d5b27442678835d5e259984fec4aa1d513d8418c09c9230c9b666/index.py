import boto3
import os
import json
import re
import uuid
import shutil
import logging


s3 = boto3.client('s3')
glue = boto3.client('glue')
ssm = boto3.client('ssm')
# S3-EVENT-CONFIG-FILE Constants
CONFIG_RULE_LOCATION = 'config/s3-event-config-rule.json'
S3_EVENT_CONFIG_FILE_RULES = "rules"
PATTERN = "pattern"
CONFIG_FILE = "config_file"
CONFIGURATIONS = "configurations"

#This unique ID can be used to track the progress 
x_correlation_id = str(uuid.uuid4())

def loginfo(message):
    log = "{} - INFO - {}".format(x_correlation_id,message)
    logging.info(log)

def logerror(message):
    log = "{} - ERROR - {}".format(x_correlation_id,message)
    logging.info(log)

def lambda_handler(event, context):
    
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    event_key = event['Records'][0]['s3']['object']['key']
    loginfo("Lambda triggered for bucket: "+bucket_name+" , event_key: "+event_key)

    # getting bucket name using parameter store
    envParam = ssm.get_parameter(Name='/env', WithDecryption=True)
    env = envParam['Parameter']['Value']
    bucketParam = ssm.get_parameter( Name='/'+env+'/scriptbucket', WithDecryption=True)
    scriptbucket = bucketParam['Parameter']['Value']

    brand, country, type = get_runtime_context(event_key)
    job_name, config_file, database = read_event_config_file(event_key)
    delete_module_from_env(scriptbucket) 
    upload_module_for_env(scriptbucket, job_name, env)
    glueJobName = env+ "_"+ job_name
    # print(glueJobName)
    
    
    
    if config_file is not None:
        runId = glue.start_job_run(JobName=glueJobName, Arguments={
            '--BUCKET': bucket_name,
            '--EVENT_KEY': event_key,
            '--BRAND': brand,
            '--COUNTRY': country,
            '--FEEDFILES_LIST': "",
            "--TYPE": type,
            '--ENV': env,
            '--DATABASE': database,
            '--X_CORRELATION_ID' : x_correlation_id
        })
        status = glue.get_job_run(JobName=glueJobName, RunId=runId['JobRunId'])
    return bucket_name, event_key


def get_runtime_context(event_key):
    details=event_key.split("/")
    brand=details[0]
    country=details[1]
    if "dark" in details[2]:
        type="dark"
    else:
        type="live"
    
    return brand,country,type
    


def read_event_config_file(key):
    config_data = None
    module_name = None
    database = None

    try:
        with open(CONFIG_RULE_LOCATION, "r") as content:
            if os.stat(CONFIG_RULE_LOCATION).st_size != 0:  # Checking if the file is empty or not
                json_data = json.load(content)

            array = []
            array = json_data[S3_EVENT_CONFIG_FILE_RULES]
            for item in array:
                if bool(re.compile(item[PATTERN]).match(key)) == True:
                    config_file = "config/" + item[CONFIG_FILE]
                    with open(config_file, "r") as content:
                        if os.stat(config_file).st_size != 0:  # Checking if the file is empty or not
                            config_data = json.load(content)
                    module_name = config_data["job_name"]
                    configurations = config_data[CONFIGURATIONS]
                    for entry in configurations:
                        database = entry["database"]

        return module_name, config_data, database
    except:
        logerror("Error Reading Configuration file for event + {}".format(key))
        raise
            

def upload_module_for_env(target_name, module_name, env):
    with open("./"+module_name+"/main.py", "rb") as file:
        s3.upload_fileobj(file, target_name, env +'/scripts/{}'.format("main.py"))
        
    # upload zip sample code to s3 bucket
    with open("./"+module_name+".zip", "rb") as zip:
        s3.upload_fileobj(zip, target_name, env +'/package/{}'.format(module_name+".zip"))

def delete_module_from_env(scriptbucket):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(scriptbucket)
    bucket.objects.all().delete()
