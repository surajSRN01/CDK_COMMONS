from constructs import Construct
import os
import time
import json
import boto3
import shutil
import utilities.log_utils as log_utils
from utilities.feed_runtime_context import FeedRuntimeContext
from aws_cdk import (
    aws_lambda as _lambda,
    Duration,
    Stack,
    aws_glue as _glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_notifications as notify,
    aws_ssm as ssm,
    aws_ec2 as ec2,
    aws_dynamodb as dynamodb
)
# Logging
logger = log_utils.get_logger()
feed_runtime_context = FeedRuntimeContext.get_instance()
feed_runtime_context.logger = logger

class MyAppStack(Stack):
    def getTime(start):
        time_taken=time.time()-start
        start=time.time()
        return time_taken

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        start = time.time()
        log_utils.print_info_logs("deployement has started")
        roles, env, script_bucket, feed_bucket, module_name = MyAppStack.read_setup_file()

# ----------------------------------BUCKET-------------------------------------------------------------------

        client = boto3.client('s3')
        
        # create s3 buckets
        bucket_script = s3.Bucket(
            self, "ScriptBucket", bucket_name=env + "-" + script_bucket)
        bucket_feed = s3.Bucket(
            self, "FeedBucket", bucket_name=env + "-" + feed_bucket)
        bucket_name=env + "-" + script_bucket
        
        
        shutil.make_archive("lambda/resources/"+module_name,"zip", "lambda/"+module_name)

        
        log_utils.print_info_logs("bucket creation done in ")
        log_utils.print_info_logs(MyAppStack.getTime(start))


# -------------------------------------------IAM_POLICIES_GLUE-------------------------------------------------------------------------

        # reading the roles from roles_policies file
        with open(roles[1], mode="r") as f:
            glue_policy = json.load(f)

        # allocating policy and resources to variables from the roles_policies file
        policy = list(glue_policy["Action"])
        rsc = list(glue_policy["Resource"])

        # creating policy statement using policy and rsc
        policy_statement = iam.PolicyStatement(
            actions=policy,
            resources=rsc
        )

        # creating role for glue
        glue_job_role = iam.Role(
            self,
            f'{env}-glue-job-role',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com')
        )

        # adding policy statement to glue role
        glue_job_role.add_to_policy(policy_statement)

        log_utils.print_info_logs("roles for glue is created in ")
        log_utils.print_info_logs(MyAppStack.getTime(start))

# ---------------------------------------GLUE_JOB----------------------------------------------------------------

        # creating glue job
        glue_job = _glue.CfnJob(self,
                                'demoGlueJob',
                               
                                role=glue_job_role.role_name,
                                glue_version="2.0",
                                name=env + "_demoGlueJob",
                                max_retries=0,
                                number_of_workers=10,
                                worker_type="G.1X",
                                
                                default_arguments={                                                          
                                        '--BUCKET': "bucket",
                                        '--EVENT_KEY': "key",
                                        '--BRAND':"brand",
                                        '--COUNTRY':"country",
                                        "--TYPE":"type",
                                        '--ENV':"env",
                                        '--DATABASE':"db",
                                    '--extra-py-files':  "s3://"+bucket_script.bucket_name+"/"+env+"/package/"+module_name+".zip" },                                  
                                command=_glue.CfnJob.JobCommandProperty(
                                    name='glueetl',
                                    python_version=os.getenv(
                                        'PYTHON_VERSION', "3"),
                                    script_location=f"s3://"+bucket_script.bucket_name + "/"+env+"/scripts/glue_script.py"
                                )
                                )
        
        log_utils.print_info_logs("glue job created in ")
        log_utils.print_info_logs(MyAppStack.getTime(start))

# -------------------------------------------IAM_POLICIES_LAMBDA-------------------------------------------------------------------------

        # reading the roles from roles_policies file
        with open(roles[0], mode="r") as f:
            lambda_policy = json.load(f)

        # allocating policy and resources to variables from the roles_policies file
        policy1 = list(lambda_policy["Action"])
        rsc1 = list(lambda_policy["Resource"])

        # creating policy statement using policy and rsc
        policy_statement1 = iam.PolicyStatement(
            actions=policy1, resources=rsc1)

        # creating role for lambda
        lambda_job_role = iam.Role(
            self,
            env+'-lambda-job-role',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'))

        # adding policy statement to lambda role
        lambda_job_role.add_to_policy(policy_statement1)

        log_utils.print_info_logs("roles for lambda is done in ")
        log_utils.print_info_logs(MyAppStack.getTime(start))

# ---------------------------------------LAMBDA_FUNCTION---------------------------------------------------------

        # creating lambda function
        lambda_func = _lambda.Function(self,
                                       "lambda_func",
                                       function_name=env + "_etl_func",
                                       runtime=_lambda.Runtime.PYTHON_3_7,
                                       handler="index.lambda_handler",
                                       code=_lambda.Code.from_asset(
                                           "lambda/resources"),
                                       timeout=Duration.seconds(60),
                                       environment=dict(
                                           BUCKET=bucket_feed.bucket_name),
                                       role=lambda_job_role
                                       )
        log_utils.print_info_logs("lambda created in ")
        log_utils.print_info_logs(MyAppStack.getTime(start))

# --------------------------------------ADD_TRIGGER_TO_S3_BUCKET----------------------------------------------

        # adding trigger to the lambda function
        # any file uploaded in s3 bucket invokes the lambda function
        
        bucket_feed.add_object_created_notification(
            notify.LambdaDestination(lambda_func)
        )

        log_utils.print_info_logs("trigger is added on s3 bucket in ")
        log_utils.print_info_logs(MyAppStack.getTime(start))

# --------------------------------------DYNAMODB----------------------------------------------------------

        table = dynamodb.Table(
            self,
            "dynamodbtable",
            table_name="ignite-dynamodb",
            partition_key=dynamodb.Attribute(
                name="id",
                type=dynamodb.AttributeType.NUMBER
            )
        )

        log_utils.print_info_logs("dynamo database created in ")
        log_utils.print_info_logs(MyAppStack.getTime(start))


# ---------------------------------------EC2_SERVICES----------------------------------------------------------------

        # creating VPC
        ETL_VPC = ec2.Vpc(self, "ETLVpc", max_azs=3)

        # creating SECURITY GROUPS
        ETL_SG = ec2.SecurityGroup(self, 'web-server-sg',
                                   vpc=ETL_VPC,
                                   allow_all_outbound=True,
                                   description='security group for a web server'

                                   )

        # creating SECURITY GROUP RULES
        ETL_SG.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(22),
            description='allow SSH access from anywhere',
        )
        ETL_SG.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(27017),
            description='mongo',
        )
        ETL_SG.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.all_traffic(),
            description='allow all traffic',
        )

        # creating LINUX AMI IMAGE
        amz_linux_ami = ec2.MachineImage.latest_amazon_linux(
            generation=ec2.AmazonLinuxGeneration.AMAZON_LINUX_2,
            edition=ec2.AmazonLinuxEdition.STANDARD,
            storage=ec2.AmazonLinuxStorage.EBS,
            virtualization=ec2.AmazonLinuxVirt.HVM
        )

        # creating KEY PAIR VALUE
        cfn_key_pair = ec2.CfnKeyPair(self, "MyCfnKeyPair",
                                      key_name=env + "_etl_key"
                                      )

        # open script to install mongo
        with open("scripts/script.sh", mode="r") as f:
            user_data = f.read()

        log_utils.print_info_logs("Requirements for EC2 instances are created in ")
        log_utils.print_info_logs(MyAppStack.getTime(start))

# ------------------------------------LAUNCH_EC2-----------------------------------------------------------

        # creating ec2 instance
        web_server = ec2.Instance(
            self,
            "myInstance1",
            instance_type=ec2.InstanceType(
                instance_type_identifier="t2.micro"),
            instance_name=env + "_etl_instance",
            machine_image=amz_linux_ami,
            vpc=ETL_VPC,
            security_group=ETL_SG,
            key_name=env + "_etl_key",
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            ),
            user_data=ec2.UserData.custom(user_data)
        )

        log_utils.print_info_logs("instance created in ")
        log_utils.print_info_logs(MyAppStack.getTime(start))

        


# ----------------------------------PARAMETER VALUE----------------------------------------------------

        # creating parameter store for storing values in aws cloud
        EC2IPParam = ssm.StringParameter(self, 'ec2IP',
                                         parameter_name='/'+env + '/myapp/ec2ipbucket',
                                         string_value=web_server.instance_public_ip,
                                         description='IP Parameter value stored',
                                         tier=ssm.ParameterTier.STANDARD
                                         )
        
        scriptBucketParam = ssm.StringParameter(self, 'scriptbucket',
                                                parameter_name='/'+env + '/myapp/scriptbucket',
                                                string_value=bucket_script.bucket_name,
                                                description='S3 ScriptBucket Parameter value stored',
                                                tier=ssm.ParameterTier.STANDARD
                                                )

        feedBucketParam = ssm.StringParameter(self, 'feedbucket',
                                              parameter_name='/'+env + '/myapp/feedbucket',
                                              string_value=bucket_feed.bucket_name,
                                              description='S3 FeedBucket Parameter value stored',
                                              tier=ssm.ParameterTier.STANDARD
                                              )

        glueParam = ssm.StringParameter(self, 'gluejob',
                                        parameter_name='/'+env + '/myapp/gluejob',
                                        string_value=glue_job.name,
                                        description='GlueJob Parameter value stored',
                                        tier=ssm.ParameterTier.STANDARD
                                        )

        envParam = ssm.StringParameter(self, 'env',
                                       parameter_name='/env',
                                       string_value=env,
                                       description='Environment is stored',
                                       tier=ssm.ParameterTier.STANDARD
                                       )

        moduleParam = ssm.StringParameter(self, 'module_name',
                                          parameter_name='/module',
                                          string_value=module_name,
                                          description='Module Name is stored',
                                          tier=ssm.ParameterTier.STANDARD
                                          )
        
        log_utils.print_info_logs("values are added in parameter store in ")
        log_utils.print_info_logs(MyAppStack.getTime(start))

        log_utils.print_info_logs("deployement is done")
        
        

    def read_setup_file():

        LOCAL_CONFIG_FILE = "utilities/build_parameter.json"
        LOCAL_CONFIG_FILE_KEY_VALUE = "ParameterValue"

        with open(LOCAL_CONFIG_FILE, "r") as etl_s3_bucket_file_config:
            # Checking if the file is empty or not
            if os.stat(LOCAL_CONFIG_FILE).st_size != 0:
                data = json.load(etl_s3_bucket_file_config)
                roles = []
                for entry in data:

                    if (entry[LOCAL_CONFIG_FILE_KEY_VALUE] != ''):
                        roles.append(entry[LOCAL_CONFIG_FILE_KEY_VALUE])
                    else:
                        # Raise the exception if the values in the file are empty.
                        print("Error")
                if roles:
                    # env [stream5qa,roles_policies/role_lambda.json, roles_policies/role_glue.json,script-bucket140,feed-bucket140,sample-code]
                    env = roles.pop(0)
                    # [roles_policies/role_lambda.json, roles_policies/role_glue.json,sample-code]
                    script_bucket = roles.pop(2)
                    feed_bucket = roles.pop(2)
                    module = roles.pop(2)
                    
                    return roles, env, script_bucket, feed_bucket, module

            else:
                print("File is Empty")
