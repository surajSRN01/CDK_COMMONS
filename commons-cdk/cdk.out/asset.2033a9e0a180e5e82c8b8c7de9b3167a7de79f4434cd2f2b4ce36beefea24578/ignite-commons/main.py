import sys
from datetime import datetime
import traceback
from pip._internal import main
main(['install','jsonpickle==2.2.0','--target','/tmp'])
sys.path.insert(0,'/tmp')
main(['install','fsspec==2021.07.0','--target','/tmp'])
sys.path.insert(1,'/tmp')
main(['install','xmltodict==0.13.0','--target','/tmp'])
sys.path.insert(2,'/tmp')
main(['install','jsonpath-ng==1.5.3','--target','/tmp'])
sys.path.insert(3,'/tmp')
main(['install','openpyxl==3.0.0','--target','/tmp'])
sys.path.insert(4,'/tmp')
main(['install','tomlkit==0.11.0','--target','/tmp'])
sys.path.insert(5,'/tmp')
main(['install','translate==3.6.1','--target','/tmp'])
sys.path.insert(6,'/tmp')
main(['install','s3fs==0.4.0','--target','/tmp'])
sys.path.insert(7,'/tmp')
main(['install','pymongo','--target','/tmp'])
sys.path.insert(8,'/tmp')
main(['install','pandas','--target','/tmp'])
sys.path.insert(9,'/tmp')

import utilities.constant as const
import utilities.log_utils as log_utils
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from utilities.ignite import process_ignite_feed as main_utils
from utilities.ignite import s3_utils_helper
from utilities.ignite.feed_runtime_context import FeedRuntimeContext, JobStatus

# Logging
logger = log_utils.get_logger()

args = {}
args = getResolvedOptions(sys.argv, ['JOB_NAME','BUCKET','EVENT_KEY','BRAND', 'COUNTRY','TYPE', 'ENV','DATABASE','X_CORRELATION_ID'])
x_correlation_id = args['X_CORRELATION_ID']

if ('test' in x_correlation_id.lower()):
    const.TEST = True

sc = SparkContext()
glueContext = GlueContext(sc.getOrCreate())
spark = glueContext.spark_session

feed_runtime_context = FeedRuntimeContext.get_instance()
feed_runtime_context.brand = args['BRAND']
feed_runtime_context.country = args['COUNTRY']
feed_runtime_context.clientType = args['TYPE']
feed_runtime_context.database = args['DATABASE']
feed_runtime_context.x_correlation_id = args['X_CORRELATION_ID']
feed_runtime_context.env = args['ENV']
feed_runtime_context.job_name = args['JOB_NAME']
feed_runtime_context.bucket = args['BUCKET']
feed_runtime_context.event_key = args['EVENT_KEY']
feed_runtime_context.logger = logger
feed_runtime_context.spark = spark
feed_runtime_context.glueContext = glueContext

def main() :
    try:
        log_utils.add_info_logs(feed_runtime_context.x_correlation_id)
        date_time = datetime.now().strftime(const.S3_FILE_DATEPATTERN)[:-3]
        const.CURRENT_TIMESTAMP = date_time
        result = main_utils.process_ignite()
        if (result):
            log_utils.print_info_logs("ignite glue processing completed!!!")
            log_utils.print_info_logs("processed folder")
            # s3_utils_helper.move_s3_objects_to_processed()
                     
        else:
            # before moving file to reject on S3 check if brand-market supports
            # multiple feed file and job needed to be terminate in absense of
            # required feed files 
            if FeedRuntimeContext.get_instance().terminate_job_silently():
                log = "brand: '{}', market: '{}' configured to upload multiple "+ \
                      "feed files which are not avalible yet, terminating job run quitely"
                log =  log.format(feed_runtime_context.brand, feed_runtime_context.country)
                log_utils.print_info_logs(log)
                pass
            else:
                log_utils.print_error_logs("move to reject folder")
                # s3_utils_helper.move_s3_objects_to_reject()
    except Exception as e:
        traceback.print_exc()
        log_utils.print_error_logs('Error in Ignite glue job')
        log_utils.print_error_logs(str(e))

        if const.PRINT_EXCEPTION:
            feedRuntimeContext = FeedRuntimeContext.get_instance()
            feedRuntimeContext.jobStatus = JobStatus.FAILED
            feedRuntimeContext.stackTrace = ''.join(traceback.TracebackException.from_exception(e).format())

        # move s3 file to rejected folder
        if not const.TEST: 
            # s3_utils_helper.move_s3_objects_to_reject()
            log_utils.print_error_logs("move to reject folder") 
        raise e
          
try :
    main()
except Exception as e:
    log_utils.print_error_logs(str(e))
