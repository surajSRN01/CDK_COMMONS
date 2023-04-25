import boto3
import pandas as pd
import utilities.constant as const
import datetime
from utilities.ignite.feed_runtime_context import FeedRuntimeContext, JobStatus
import utilities.prop_reader as prop_reader
import utilities.log_utils as log_utils
import xmltodict
from utilities import validator as validator


feed_runtime_context = FeedRuntimeContext.get_instance()
    
# function to move the feed file to destination folder
def move_s3_object(bucket, event_key, destination_path,is_filename_with_timestamp) :
    feed_runtime_context = FeedRuntimeContext.get_instance()
    try :
        s3_resource = boto3.resource('s3')
        dt_string = const.CURRENT_TIMESTAMP if const.CURRENT_TIMESTAMP !="" else datetime.now().strftime(const.S3_FILE_DATEPATTERN)[:-3]
        file_paths = event_key.split(sep="/")
        file_name_with_ext = file_paths[-1]
        s3_folder_name = event_key.replace(file_name_with_ext,'') 
        s3_folder_name = s3_folder_name.replace('working/','')  if 'working/' in s3_folder_name else s3_folder_name
        file_name_with_ext_list = file_name_with_ext.split('.', -1)
        ext = file_name_with_ext_list[len(file_name_with_ext_list)-1]
        file_name = file_name_with_ext.replace('.'+ext, '')
        destination_folder = prop_reader.get_prop(const.s3_section, destination_path)
        processed_file = dt_string + '-'+ file_name + '.' + ext if is_filename_with_timestamp else file_name_with_ext
        destination = s3_folder_name + destination_folder +'/'+processed_file

        current_config_id = ""
        if feed_runtime_context.current_configuration is not None and \
           "id" in feed_runtime_context.current_configuration:
            current_config_id = feed_runtime_context.current_configuration["id"]
            
        log_file_destination = s3_folder_name + destination_folder +'/'+ dt_string + '-' + file_name + '-'+ current_config_id + '-logs.txt' 
        feed_runtime_context.log_file_path = log_file_destination
    
        event_key = event_key if const.IS_WORKING_FOLDER_EXISTS == False else get_working_folder_path(event_key) 
        source = bucket+'/'+event_key
        log_utils.print_info_logs('s3 bucket : '+bucket+' file_name : '+file_name+' destination : '+destination+' log_file_destination : '+log_file_destination)
        s3_resource.Object(bucket, destination).copy_from(CopySource= source)
        s3_resource.Object(bucket, event_key).delete()
        if 'working' not in destination_folder:
           
            if not feed_runtime_context.has_consolidate_log_file:
                """
                Create one log file for multiple feed files if market support multiple feed file
                At this point all the feed files will be avalible
                """
                data = "\n".join(log_utils.get_info_logs())
                object = s3_resource.Object(bucket, log_file_destination)
                object.put(Body=data)
                feed_runtime_context.has_consolidate_log_file = True

        log_utils.print_info_logs(file_name+' has been moved to '+ destination_folder + ' folder successfully')

         # create log file ig the Dealer V1 Glue job because of internal error
        if const.PRINT_EXCEPTION and feed_runtime_context.jobStatus == JobStatus.FAILED:
            stackTrace_file = s3_folder_name + destination_folder +'/'+ dt_string + '-' + file_name + '-'+'exception.txt' 
            create_exception_log_file(s3_resource, bucket, feed_runtime_context.stackTrace, \
                                        stackTrace_file)
    
    except Exception as e :
        raise e

# function to Delete S3 Object from source
def delete_s3_object(bucket, event_key):
    feed_runtime_context = FeedRuntimeContext.get_instance()
    
    try:
        s3_resource = boto3.resource('s3')
        file_paths = event_key.split(sep="/")
        file_name_with_ext = file_paths[-1]
        s3_resource.Object(bucket, event_key).delete()
        log_utils.print_info_logs(file_name_with_ext+' has been delete successfully!!')
    except Exception as e :
        log_utils.print_error_logs('-Error while deleting the feed file in s3 bucket')
        raise e

# function to read S3 Object
def read_s3_object():
    feed_runtime_context = FeedRuntimeContext.get_instance()
    
    try:
        log_utils.print_info_logs("Reading feed file from S3...")
        if(".xml" in feed_runtime_context.event_key ):
            event_key = feed_runtime_context.event_key if const.IS_WORKING_FOLDER_EXISTS == False else get_working_folder_path(feed_runtime_context.event_key)
            s3 = boto3.client('s3')
            data = s3.get_object(Bucket=feed_runtime_context.bucket, Key=event_key)
            xml_data = data['Body'].read()
            obj_dict = xmltodict.parse(xml_data)
            return obj_dict
        elif(".txt" in feed_runtime_context.event_key or ".csv" in feed_runtime_context.event_key):
            event_key = feed_runtime_context.event_key if const.IS_WORKING_FOLDER_EXISTS == False else get_working_folder_path(feed_runtime_context.event_key)
            s3_file = "s3://" + feed_runtime_context.bucket + "/" + event_key
            s3 = boto3.client('s3')
            data = s3.get_object(Bucket=feed_runtime_context.bucket, Key=event_key)
            return data['Body'].read()
        else:
            event_key = feed_runtime_context.event_key if const.IS_WORKING_FOLDER_EXISTS == False else get_working_folder_path(feed_runtime_context.event_key)
            s3_file = "s3://" + feed_runtime_context.bucket + "/" + event_key
            return  pd.read_excel(s3_file, dtype=str, sheet_name= None, engine='openpyxl')

    except Exception as e :
        raise e
    
# function to list S3 Object
def list_s3_object(bucket, bucketkey):
    try:
        s3_client = boto3.client('s3')
        result = s3_client.list_objects_v2(Bucket=bucket,Prefix=bucketkey,Delimiter='/')
        return result
    except Exception as e :
        raise e

# function to S3 Working folder cleanup
def s3_working_folder_cleanup( feed_path_prefix):
    
    try:
        bucketkey = feed_path_prefix + '/working/'
        upload_file_paths = feed_runtime_context.event_key.split(sep="/")
        upload_file_name_with_ext = upload_file_paths[-1]
        s3_working_folder_object = list_s3_object(feed_runtime_context.bucket, bucketkey)
        if ('Contents' in s3_working_folder_object):
            for s3_object in s3_working_folder_object["Contents"]:
                key = s3_object["Key"]
                file_paths = key.split(sep="/")
                file_name_with_ext = file_paths[-1]
                if(upload_file_name_with_ext == file_name_with_ext):
                    feed_runtime_context.logger.info(const.x_correlation_id +feed_runtime_context.x_correlation_id+ '- Upload File and Working Folder File Name is same!!')
                else:
                    delete_s3_object(feed_runtime_context.bucket, key)
                    feed_runtime_context.logger.info(const.x_correlation_id +feed_runtime_context.x_correlation_id+ '- Upload File and Working Folder File Name is different!!')

            feed_runtime_context.logger.info(const.x_correlation_id +feed_runtime_context.x_correlation_id+ '- Working Folder Cleanup Done Successfully!!') 
        else:
           feed_runtime_context.logger.info(const.x_correlation_id +feed_runtime_context.x_correlation_id+ '- Working Folder Not Found') 
    except Exception as e :
        raise e

def get_working_folder_path(event_key):
    file_paths = event_key.split(sep="/")
    file_name_with_ext = file_paths[-1]
    return event_key.replace(file_name_with_ext,'')+'working/'+ file_name_with_ext

def check_objects_exists(s3_objects, prefix):
    """
    Check if object exists on S3 for given object_names, return the missing objects
    """
    s3_resource = boto3.resource('s3')
    s3_bucket = s3_resource.Bucket(feed_runtime_context.bucket)
    missing_s3_object = []
    
    for s3_object in s3_objects:
        objs = list(s3_bucket.objects.filter(Prefix=prefix+"/"+s3_object))
        if not check_s3_object_exists_in_s3_object_collection(objs, prefix+"/"+s3_object):
            missing_s3_object.append(s3_object)

    return missing_s3_object

def check_s3_object_exists_in_s3_object_collection(s3_object_collection, s3_object_name):
    if s3_object_collection == None:
        return False
    try:
        # option 1: s3_object_collection is a plain and simple list 
        return len(s3_object_collection) is 1 and s3_object_collection[0].key == s3_object_name
    except Exception as e:
        # option 2: s3_object_collection is a collection of s3_object summary
        s3_object_names = list(map(lambda obj:obj.key, s3_object_collection.all()))
        # must have what we are looking at
        return len(s3_object_names) == 1 and s3_object_name in s3_object_names     

def create_exception_log_file(s3_resource, bucket, stackTrace, exception_file):
    object = s3_resource.Object(bucket, exception_file)
    object.put(Body=stackTrace)      