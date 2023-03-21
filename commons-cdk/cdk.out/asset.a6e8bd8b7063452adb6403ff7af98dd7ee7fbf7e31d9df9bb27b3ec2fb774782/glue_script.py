import main_process_flow as flow
from awsglue.transforms import *
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import sys



args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
ssm = boto3.client('ssm')


envParam = ssm.get_parameter(Name='/env', WithDecryption=True)
env = envParam['Parameter']['Value']

bucketparameter = ssm.get_parameter(Name='/'+env+'/myapp/feedbucket', WithDecryption=True)
bucket = bucketparameter['Parameter']['Value']

dbParam = ssm.get_parameter(Name='/database', WithDecryption=True)
db = dbParam['Parameter']['Value']


flow.main(bucket,db)
job.commit()
