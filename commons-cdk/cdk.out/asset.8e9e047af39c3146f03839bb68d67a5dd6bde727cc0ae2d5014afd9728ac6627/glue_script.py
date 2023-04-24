import main_process_flow as flow
from awsglue.transforms import *
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import sys


sc = SparkContext()
glueContext = GlueContext(sc.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME','BUCKET','EVENT_KEY','BRAND', 'COUNTRY','TYPE', 'ENV','DATABASE'])
job.init(args["JOB_NAME"], args)
ssm = boto3.client('ssm')

print("request context : ",args)
# flow.main(args["BUCKET"],args["DATABASE"])
job.commit()
