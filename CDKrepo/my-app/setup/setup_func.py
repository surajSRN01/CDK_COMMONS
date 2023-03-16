import os
import json
LOCAL_CONFIG_FILE = "setup/local_setup.json"
LOCAL_CONFIG_FILE_KEY_VALUE = "ParameterValue"


def read_setup_file():
    with open(LOCAL_CONFIG_FILE, "r") as etl_s3_bucket_file_config:
        if os.stat(LOCAL_CONFIG_FILE).st_size != 0:  # Checking if the file is empty or not
            data = json.load(etl_s3_bucket_file_config)
            roles = []
            for entry in data:

                if (entry[LOCAL_CONFIG_FILE_KEY_VALUE] != ''):
                    roles.append(entry[LOCAL_CONFIG_FILE_KEY_VALUE])
                else:
                    # Raise the exception if the values in the file are empty.
                    print("Error")
            if roles:
                env = roles.pop(0)  # env [stream5qa,roles_policies/role_lambda.json, roles_policies/role_glue.json,script-bucket140,feed-bucket140,sample-code]
                script_bucket=roles.pop(2) #[roles_policies/role_lambda.json, roles_policies/role_glue.json,sample-code]
                feed_bucket=roles.pop(2)
                module=roles.pop(2)
                db=roles.pop(2)
                
                return roles,env,script_bucket,feed_bucket,module,db
            
        else:
            print("EmptyFile")
