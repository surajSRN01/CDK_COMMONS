# BRANDS = ["nissan", "datsun", "infiniti" ]

mongo_section = 'mongo'
SSM = 'ssm'
S3 = 's3'
SQS = 'sqs'
SNS = 'sns'
FILE_EXTENSION = ".json"
HYPHAN = "-"
UNDER_SCORE = "_"
SEPERATOR = "/"
DECIMAL = "."
config_section = "configuration"
PROPERTIES_PATH = 'properties/'
CONFIGURATION_PATH = 'configurations/'
MODEL_PATH = 'models/'
PARSER_CONFIGURATION_PATH="parser-config/"
MESSAGE_ID = 'MessageId'
x_correlation_id ='x_correlation_id : '
PROPERTIES_PATH = 'properties/'
email_section = 'ses'
WORKING_FOLDER = 's3.working.folder'
PROCESSED_FOLDER = 's3.processed.folder'
UPLOAD_FOLDER ='s3.upload.folder'
REJECTED_FOLDER ='s3.rejected.folder'
s3_section = 's3folder'
sns_section = 'sns'
client_key_section = "clientkeys"
apigee_section = "apigee"
S3_FILE_DATEPATTERN = "%Y%m%d%H%M%S%f"  
CURRENT_TIMESTAMP=""
IS_WORKING_FOLDER_EXISTS = False
SPARK_MONGODB_OUTPUT_URI = "spark.mongodb.output.uri"
APPEND = "append"
DEFAULT_SOURCE = "com.mongodb.spark.sql.DefaultSource"

TEST = False
MONGO = "mongo"
URI = "uri"
PROTOCOL = "https://"

##########

REGEX_FILE_PATH="(brand)/[a-z]+-?[a-z]*/(ignite|_darkignite)/(upload)/(.*?)"
REGEX_FILE_NAME="ignites_[a-zA-Z]+_[a-zA-Z]{2}_.*\.xlsx"
FILE_PATH=""
UPLOAD_PATH=""

CONFIG_ID=""
CURRENT_CONFIG={}
PARSER_CONFIG={}                    
FILE_ENCODING=""

PROCESS_DEALER_MAIL_TO_FEEDS = "mailto-ignite-mapping.csv"
CLIENT_HEADER = "clientKey"
API_HEADER = "apiKey"
APIGEE_CLEAR_CACHE_PREFIX_SEPARATOR = "__"
DEFAULT_ENCODING = "utf-8"
holiday_section = "holiday"
MOCK_URL_ENABLED = False
HOLIDAY_URL = None

######
# flag to turn off/on for exception stacktrace log file generation
# Set to false on higer enviroment
PRINT_EXCEPTION = True

###
# MAX_THREAD Required for JP Special openingHourRule
MAX_THREAD = 100