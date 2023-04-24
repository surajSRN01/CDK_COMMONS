import main_process_flow as flow
from awsglue.transforms import *
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import sys

args = getResolvedOptions(sys.argv, [])
print("sys : ",args)
sc = SparkContext()
glueContext = GlueContext(sc.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
print("job : ",job)
print("sys : ",sys.argv)
args = getResolvedOptions(sys.argv, ['JOB_NAME','BUCKET','EVENT_KEY','DATABASE'])
job.init(args["JOB_NAME"], args)
ssm = boto3.client('ssm')
print("job : ",job)
print("args : ",args)
bucket=args["BUCKET"]
db=args["DATABASE"]

print("inside the glue script",bucket," db ",db)
# flow.main(bucket,db)
job.commit()
