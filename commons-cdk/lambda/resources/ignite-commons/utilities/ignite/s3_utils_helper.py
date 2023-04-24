"""
Handle all or nothing senerio in case of multiple feed files allowed 
when moving one files from source to target.
"""

from typing import List
from aggregators.rules.aggregator_utils.UpdateDeatilsLog import UpdateDetailLog

from models.FileType import FileType
from commons.aggregator_result import AggregatorResult
from utilities import constant, log_utils, s3_utils
from utilities.ignite.feed_runtime_context import FeedRuntimeContext
from utilities import prop_reader as prop_reader

def move_s3_objects_to_reject():
    feed_runtime_context = FeedRuntimeContext.get_instance()
    log_message = "Rejected file {file_name} backed up and deleted"
    move_s3_objects(constant.REJECTED_FOLDER, log_message)
    
def move_s3_objects_to_processed():
    log_message = "Processing for the file {file_name} completed moved to processed"
    move_s3_objects(constant.PROCESSED_FOLDER, log_message)

def move_s3_objects(target, log_message):
    feed_runtime_context = FeedRuntimeContext.get_instance()
    if target == constant.PROCESSED_FOLDER:
        for key in feed_runtime_context.feed_file_aggregation_results_map.keys():
            aggregation_results: List[AggregatorResult] = feed_runtime_context.feed_file_aggregation_results_map[key]
            for aggregation_result in aggregation_results:
                #  UpdateDetailLog.updateLog(aggregation_result)
                log_utils.get_info_logs().extend(aggregation_result.parserResult.report.logs)
                log_utils.get_info_logs().append("")
                log_utils.get_info_logs().append("")
        
    if not feed_runtime_context.terminate_job_silently():
        # move all the dependent feed files to target, all of nothing
        for file in feed_runtime_context.get_dependent_feed_files_as_event_key():
            file: FileType = file
            feed_runtime_context.event_key = file.name
            try:
                s3_utils.move_s3_object(feed_runtime_context.bucket, feed_runtime_context.event_key, target, True)
                log_utils.print_info_logs(log_message.format(file_name=feed_runtime_context.event_key))
            except Exception as e:
                if file.isOptional:
                    optional_file_name = file.name.split("/")[-1]
                    log_utils.print_info_logs("optional feed file:{}, not exist, ignoring feed file".format(optional_file_name))
                else:
                    log_utils.print_error_logs('-Error while moving the feed file in s3 bucket '+ target + ' folder')
                    raise e
        
def move_file_to_s3(feed_file_to_process: FileType):
    """
    when pulling an optional feed file from S3, its fine to not have those on S3.\n
    Glue job should not complain about missing an optional file
    Else throw an exception is a required file is missing
    """
    feed_runtime_context = FeedRuntimeContext.get_instance()
    
    if constant.TEST:   
        print("#### Running in test mode, does not support file moving on S3")
        return
    
    try:
        feed_runtime_context.event_key = feed_runtime_context.upload_path +"/"+ feed_file_to_process.name
        # move s3 file to working folder
        s3_utils.move_s3_object(feed_runtime_context.bucket, feed_runtime_context.event_key, \
                                constant.WORKING_FOLDER, False)
    except Exception as e:
        if feed_file_to_process.isOptional:
            optional_file_name = feed_runtime_context.event_key.split("/")[-1]
            log_utils.print_info_logs("optional feed file:{}, not exist, ignoring feed file".format(optional_file_name))
        else:
            log_utils.print_error_logs('-Error while moving the feed file in s3 bucket '+ constant.WORKING_FOLDER + ' folder')
            raise e
    
def read_file_from_s3(feed_file_to_process:FileType):
    """
    reading an optional feed file from S3, its fine to not have those on S3.\n
    Glue job should not complain about missing an optional file
    Else throw an exception is a required file is missing
    """
    feed_runtime_context = FeedRuntimeContext.get_instance()    
    try:
        feed_runtime_context.event_key = feed_runtime_context.upload_path +"/"+ feed_file_to_process.name
        return s3_utils.read_s3_object()
    except Exception as e:
        if feed_file_to_process.isOptional:
            optional_file_name = feed_runtime_context.event_key.split("/")[-1]
            log_utils.print_info_logs("optional feed file:{}, not exist, ignoring feed file".format(optional_file_name))
            return None
        else:
            log_utils.print_error_logs(str(e))
            log_utils.print_error_logs('-Error while reading feed file from s3 bucket '+ constant.WORKING_FOLDER + ' folder')
            raise e    