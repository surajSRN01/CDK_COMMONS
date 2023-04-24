import boto3
import os
import json
import re


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
    moduleParam = ssm.get_parameter(Name='/module', WithDecryption=True)
    module_name = moduleParam['Parameter']['Value']
    ec2IPparam = ssm.get_parameter(Name='/'+env+'/myapp/ec2ipbucket', WithDecryption=True)
    ec2IP= ec2IPparam['Parameter']['Value']

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
    glueJobName = glueParam['Parameter']['Value']

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    event_key = event['Records'][0]['s3']['object']['key']
    print(" lambda triggered for bucket: " +
          bucket_name+" , event_key: "+event_key)
<<<<<<< HEAD

    config_file = read_event_config_file(event_key)
    database = extract_db(config_file)
    
    print("data fetched successfully : "+config_file+" and "+database)

    brand, country, type = get_runtime_context(event_key)

    runId = glue.start_job_run(JobName=glueJobName, Arguments={
        '--BUCKET': bucket_name,
        '--EVENT_KEY': event_key,
        '--BRAND': brand,
        '--COUNTRY': country,
        '--FEEDFILES_LIST': "",
        "--TYPE": type,
        '--ENV': env,
        '--DATABASE': database})
    print(runId)
    status = glue.get_job_run(JobName=glueJobName, RunId=runId['JobRunId'])
    print(status)
=======

    config_file = read_event_config_file(event_key)
    if config_file is not None:
        database = extract_db(config_file)
        print("data fetched successfully : "+config_file+" and "+database)

        brand, country, type = get_runtime_context(event_key)
        coId = "test-mongo-"+ec2IP
        runId = glue.start_job_run(JobName=glueJobName, Arguments={
            '--BUCKET': bucket_name,
            '--EVENT_KEY': event_key,
            '--BRAND': brand,
            '--COUNTRY': country,
            '--FEEDFILES_LIST': "",
            "--TYPE": type,
            '--ENV': env,
            '--DATABASE': database,
            '--X_CORRELATION_ID' : coId
        })
        print(runId)
        status = glue.get_job_run(JobName=glueJobName, RunId=runId['JobRunId'])
        print(status)
>>>>>>> 041d356e98e8eeec8f5d545e2c53a43ca8ec988e
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
    


def read_event_config_file(key):

    with open(CONFIG_RULE_LOCATION, "r") as content:
        if os.stat(CONFIG_RULE_LOCATION).st_size != 0:  # Checking if the file is empty or not
            json_data = json.load(content)

        array = []
        array = json_data[S3_EVENT_CONFIG_FILE_RULES]
        for item in array:
<<<<<<< HEAD
            # if bool(re.search(item[PATTERN], key)) == True:
            config_file = item[CONFIG_FILE]
=======
            if bool(re.compile(item[PATTERN]).match(key)) == True:
                config_file = item[CONFIG_FILE]
            else:
                config_file = None
>>>>>>> 041d356e98e8eeec8f5d545e2c53a43ca8ec988e
            return config_file


def extract_db(config_file):

    with open(config_file, "r") as content:
        if os.stat(config_file).st_size != 0:  # Checking if the file is empty or not
            file_data = json.load(content)
    configurations = file_data[CONFIGURATIONS]
    for entry in configurations:
        database = entry["database"]

    return database
