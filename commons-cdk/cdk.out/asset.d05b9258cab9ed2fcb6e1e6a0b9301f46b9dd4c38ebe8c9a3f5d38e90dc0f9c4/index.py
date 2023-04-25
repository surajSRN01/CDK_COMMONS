import boto3
import os
import json
import re
import uuid
import shutil


s3 = boto3.client('s3')
glue = boto3.client('glue')
ssm = boto3.client('ssm')
# S3-EVENT-CONFIG-FILE Constants
CONFIG_RULE_LOCATION = 'config/s3-event-config-rule.json'
S3_EVENT_CONFIG_FILE_RULES = "rules"
PATTERN = "pattern"
CONFIG_FILE = "config_file"
CONFIGURATIONS = "configurations"


def lambda_handler(event, context):

    envParam = ssm.get_parameter(Name='/env', WithDecryption=True)
    env = envParam['Parameter']['Value']
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    event_key = event['Records'][0]['s3']['object']['key']
    print(" lambda triggered for bucket: " +
          bucket_name+" , event_key: "+event_key)

    # getting bucket name using parameter store
    bucketParam = ssm.get_parameter( Name='/'+env+'/scriptbucket', WithDecryption=True)
    target_name = bucketParam['Parameter']['Value']

    job_name, config_file = read_event_config_file(event_key, target_name, env)
    glueJobName = env+ "_"+ job_name
    print(glueJobName)
    
    
    
    if config_file is not None:
        
        database = extract_db(config_file)
        print("data fetched successfully : "+config_file+" and "+database)

        brand, country, type = get_runtime_context(event_key)
        x_correlation_id = str(uuid.uuid4())
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
        
        print(runId)
        status = glue.get_job_run(JobName=glueJobName, RunId=runId['JobRunId'])
        print(status)
    return bucket_name, event_key


def get_runtime_context(event_key):
    details=event_key.split("/")
    brand=details[0]
    country=details[1]
    # brand/au/ignite/upload/ignite_personList_AU_20230410.xlsx
    if "dark" in details[2]:
        type="dark"
    else:
        type="live"
    
    return brand,country,type
    


def read_event_config_file(key,target_name, env):

    with open(CONFIG_RULE_LOCATION, "r") as content:
        if os.stat(CONFIG_RULE_LOCATION).st_size != 0:  # Checking if the file is empty or not
            json_data = json.load(content)

        array = []
        array = json_data[S3_EVENT_CONFIG_FILE_RULES]
        for item in array:
            if bool(re.compile(item[PATTERN]).match(key)) == True:
                config_file = item[CONFIG_FILE]
                with open(config_file, "r") as content:
                    if os.stat(config_file).st_size != 0:  # Checking if the file is empty or not
                        config_data = json.load(content)
                module_name = config_data["job_name"]
                delete_objects_from_bucket(target_name) 
                upload_file(target_name, module_name, env)
        
                return module_name, config_file
                
            else:
                return None, None
            

def upload_file(target_name, module_name, env):
    # upload glue script to the s3 bucket
    with open("./"+module_name+"/main.py", "rb") as file:
        s3.upload_fileobj(file, target_name, env +'/scripts/{}'.format("main.py"))

    shutil.make_archive("./"+module_name,
                            "zip","./"+module_name)
            
    # upload zip sample code to s3 bucket
    with open("./"+module_name+".zip", "rb") as zip:
        s3.upload_fileobj(zip, target_name, env +'/package/{}'.format(module_name+".zip"))

def extract_db(config_file):

    with open(config_file, "r") as content:
        if os.stat(config_file).st_size != 0:  # Checking if the file is empty or not
            file_data = json.load(content)
    configurations = file_data[CONFIGURATIONS]
    for entry in configurations:
        database = entry["database"]

    return database

def delete_objects_from_bucket(target_name):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(target_name)
    bucket.objects.all().delete()
