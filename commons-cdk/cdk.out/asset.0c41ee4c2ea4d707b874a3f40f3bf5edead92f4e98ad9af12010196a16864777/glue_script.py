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

bucket=args["--bucket"]
db=args["--database"]

print("inside the glue script")
# flow.main(bucket,db)
job.commit()
