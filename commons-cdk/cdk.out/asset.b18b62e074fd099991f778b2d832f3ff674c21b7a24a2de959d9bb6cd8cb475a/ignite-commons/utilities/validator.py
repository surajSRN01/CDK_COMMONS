
import json
import math
import re

import numpy
from models.FileType import FileType
import utilities.constant as const
import utilities.log_utils as log_utils
import utilities.s3_utils as s3_utils
from utilities.ignite.feed_runtime_context import FeedRuntimeContext

MANDATORY_VALIDATION_MSG = "Field '{fieldName}' cannot be NULL."
LENGTH_VALIDATION_MSG = "Field '{fieldName}' cannot be NULL and it's length cannot be greater than {length}."
REGEX_VALIDATION_MSG = "Field '{fieldName}' does not match regex '{regex}'"

invalid_row_index=[]
feed_runtime_context = FeedRuntimeContext.get_instance()
    


def is_valid_file_path():
    try:
        file_path = feed_runtime_context.event_key
        base_path = file_path.rpartition("/")[0]

        feed_runtime_context.upload_path = base_path
        feed_runtime_context.current_file_path = file_path
        
        """
        Regex working non-determintsic on UAT2 vs Stream5
        This check is non-required because the glue job invoked when a file
        put on valid S3 folder 
        re.compile(const.REGEX_FILE_PATH).match(file_path)
        """
        return True
    except Exception as e:        
        raise e 

def get_valid_file_name():
    try:
        file_path=feed_runtime_context.event_key
        file_name=file_path.split("/")[-1]
        return re.compile(const.REGEX_FILE_NAME).match(file_name)
    except Exception as e:        
        raise e

def get_mandatory_columns(fields, mandatory_columns):
    try:        
        for field in fields:
            if "type" in field and field["type"]=="object":
                get_mandatory_columns(field["fields"],mandatory_columns)
            if "validations" in field and "mandatory" in field["validations"] and field["validations"]["mandatory"] == True: 
                mandatory_columns.append(field["target"])
    except Exception as e: 
        raise e

def remove_row_column_nan(current_sheet_pdf):
    try:
        current_sheet_pdf.fillna(value=numpy.nan, inplace=True)# fill all empty values(NaN,NONE,nan) with nan
        #remove nan rows 
        # drop all row if all column value is NAN
        current_sheet_pdf.dropna(axis=0, how='all', inplace=True)
        # drop all Column if all row value is NAN including column header
        # current_sheet_pdf.dropna(axis= 1, how='all', inplace=True)
        return current_sheet_pdf

    except Exception as e: 
        raise e

def is_valid_row(sheet, row, columns, row_index, list_of_msg):
    try:        
        mandatory_column = get_mandatory_columns(sheet)
        for column_name in columns:
            column_value = row['record'][column_name]
            if column_name in mandatory_column and column_value == 'NaN':
                list_of_msg += column_name + ", "
                log_utils.add_info_logs("Row Number : "+ str(row_index) +" >> Field : "+ column_name+" cannot be Empty.")
            # have to check if there is any other conditional rows to be valid
        return True if list_of_msg == '' else False
    except Exception as e :
        log_utils.print_error_logs(' - Error while validating Row!!')
        raise e

def validate_ignite_feed_and_find_parser_config(configuration):
    feed_runtime_context = FeedRuntimeContext.get_instance()
    try:       
        is_valid_feed=False
        ## all configuration's feedpathprefix will be same directory so only one feedpath prefix is enough to check feed path file
        feed_path_prefix = configuration["configurations"][0]["feedPathPrefix"]
        file_name_with_ext = feed_runtime_context.event_key.split(sep="/")[-1]
        s3_base_path = feed_runtime_context.event_key.replace(file_name_with_ext,'')
        id_required_feed_dict = {}
        id_configuration_dict = {}

        for config in configuration["configurations"]:
            id = config["id"]
            id_configuration_dict.update({id: config})
            id_required_feed_dict.update({id:config['requiredFeeds']})
            
        for id in id_required_feed_dict:
            for reqd_feed in id_required_feed_dict[id]:
                encoding = reqd_feed["encoding"] if "encoding" in reqd_feed else ""
                file_pattern = reqd_feed["filePattern"]
                
                is_parser_config_retrived = False
                if len(id_required_feed_dict[id]) > 1:
                    files_patterns = list(map(lambda x: x["filePattern"],id_required_feed_dict[id]))
                    check = file_name_with_ext in files_patterns
                    is_valid_feed = True if s3_base_path == feed_path_prefix and check else False
                    file_pattern = file_name_with_ext
                    for required_feed in id_required_feed_dict[id]:
                        if required_feed["filePattern"] == file_name_with_ext:
                            is_parser_config_retrived = True
                            feed_runtime_context.current_parser_configs = required_feed["parserConfigurations"]
                            if "encoding" in required_feed:
                                feed_runtime_context.encoding = required_feed["encoding"]
                                encoding = feed_runtime_context.encoding
                            break        
                else:
                    py_file_pattern= str(file_pattern).replace("\\", "")
                    check = re.compile(py_file_pattern).match(file_name_with_ext)                                    
                    is_valid_feed = True if s3_base_path == feed_path_prefix and check else False 
                
                if is_valid_feed:
                    log_utils.print_info_logs(feed_runtime_context.brand+const.UNDER_SCORE+feed_runtime_context.country+" - Feed "+file_name_with_ext+ " match "+file_pattern)
                    log_utils.print_info_logs(feed_runtime_context.brand+const.UNDER_SCORE+feed_runtime_context.country+" - Validation result for "+  feed_runtime_context.brand+const.UNDER_SCORE+feed_runtime_context.country +" feeds: "+ str(is_valid_feed))
                    
                    feed_runtime_context.current_configuration = id_configuration_dict[id]
                    feed_runtime_context.current_required_feeds_config = id_required_feed_dict[id]
                    
                    if not is_parser_config_retrived:
                        feed_runtime_context.current_parser_configs = reqd_feed["parserConfigurations"]
                    
                    feed_runtime_context.encoding = encoding    
                    return True

        if(not is_valid_feed):
            log_utils.add_info_logs("The file name doesn't match with the expected file naming convention.")
            log_utils.print_info_logs(feed_runtime_context.brand+const.UNDER_SCORE+feed_runtime_context.country+" - Validation result for "+  feed_runtime_context.brand+const.UNDER_SCORE+feed_runtime_context.country +" feeds: false")
            # If we don't have valid feed file after checking all level of configuration
            # throw the exception
            feed_runtime_context.dependent_feed_files.append(FileType(file_name_with_ext,False))
            raise Exception("Invalid Feed file")
            
        return is_valid_feed
    except Exception as e:
        log_utils.print_error_logs(' - Error while validating Ignite Feed File!!')
        raise e


def field_validation(value, fieldConfig):   
    if("validations" not in fieldConfig):
        return True, ""
    validations = fieldConfig["validations"]
    if("mandatory" in validations and validations["mandatory"] and is_nan(value)):
        return False, MANDATORY_VALIDATION_MSG.format(fieldName = fieldConfig["target"])

    if("length" in validations and (value =="" or len(value) < int(validations["length"]))):
        error_message = LENGTH_VALIDATION_MSG.format(fieldName = fieldConfig["target"], length = validations["length"])
        return False, error_message

    if("regex" in validations and validations["regex"] != ""):
        regex = re.compile(validations["regex"])
        if(regex.fullmatch(value) == None):
            error_message = REGEX_VALIDATION_MSG.format(fieldName = fieldConfig["target"], regex = validations["regex"])
            return False, error_message
        
    return True, ""

def is_nan(cell_value) -> bool:
    math_check = (isinstance(cell_value, float) and math.isnan(cell_value))
    nan_check = (cell_value == 'NaN') or (cell_value =='nan')
    object_check = cell_value == None
    empty_check = (isinstance(cell_value, str) and cell_value.strip() == '')
    return math_check or nan_check or empty_check or object_check