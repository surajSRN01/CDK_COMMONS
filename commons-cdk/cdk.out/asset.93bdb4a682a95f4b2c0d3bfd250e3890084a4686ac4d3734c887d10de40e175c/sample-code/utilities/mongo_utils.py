import sys
from pip._internal import main
main(['install', 'pymongo', '--target', '/tmp'])
sys.path.insert(0, '/tmp')
from pymongo import MongoClient
import boto3

def save_to_mongo(data_document):
    ssm = boto3.client('ssm')

    envParam = ssm.get_parameter(Name='/env', WithDecryption=True)
    env = envParam['Parameter']['Value']

    ec2parameter = ssm.get_parameter(
    Name='/'+env+'/myapp/ec2ipbucket', WithDecryption=True)
    ec2IP = ec2parameter['Parameter']['Value']

    parameter_store_live_host = ec2IP
    parameter_store_live_port = "27017"
    database_live = "mongoo-data"
    database_collection = "secondCollection"

    host_prop = parameter_store_live_host
    port_prop = parameter_store_live_port
    database_prop = database_live
    collection = database_collection

    mongo_uri = 'mongodb://'+host_prop+':'+port_prop
    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client[database_prop]
    mongo_collection = mongo_db[collection]

    for i in data_document:
        mongo_save_result = mongo_collection.insert_one(i)
    
    return True
