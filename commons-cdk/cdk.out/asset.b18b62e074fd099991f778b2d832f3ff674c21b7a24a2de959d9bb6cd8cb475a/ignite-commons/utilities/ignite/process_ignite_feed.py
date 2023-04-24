
import traceback
from typing import List

from models.FileType import FileType
import utilities.aggregator_utils as aggregator_utils
from utilities import aggregator_utils as aggregator_utils
import utilities.constant as const
import utilities.ignite.mongo_helper as mongo_helper
import utilities.log_utils as log_utils
import utilities.prop_reader as prop_reader
import utilities.s3_utils as s3_utils
import utilities.validator as validator
from commons.parser_result import ParserResult
from exception.cache_clear_operation_exception import CacheClearOpeationException
from exception.error_code import ErrorCode
from exception.etl_exception import EtlException
from exception.repository_operation_exception import RepositoryOperationException
from utilities.ignite.feed_runtime_context import (FeedRuntimeContext, Status)
from utilities.parsers.base.parser_base import ParserBase
from utilities.parsers.base.parser_builder import get_parser_by_feed_type
from utilities.ignite import s3_utils_helper as s3_utils_helper
import utilities.transformer as transformer

feed_runtime_context = FeedRuntimeContext.get_instance()
    
def read_config():
    try:
        config_filename = "ignite"+ const.HYPHAN+ feed_runtime_context.brand+ const.HYPHAN+ feed_runtime_context.country+ const.FILE_EXTENSION
        absolute_file_path = const.CONFIGURATION_PATH + feed_runtime_context.brand + const.SEPERATOR + config_filename
        market_config_file= prop_reader.get_config_prop(
                const.config_section,'read-configuation' , absolute_file_path)
        set_data_source_type(market_config_file)
        feed_runtime_context.current_market_config = market_config_file
        return market_config_file        
    except Exception as e :
        raise e


def read_parser_configuration( parser_config_filename):
    country= str(feed_runtime_context.country).replace("-","_")
    parser_config_path = "{parser_config_folder}{brand}/{country}/{file_name}".format(
            parser_config_folder=const.PARSER_CONFIGURATION_PATH, brand=feed_runtime_context.brand, 
            country=country, file_name=parser_config_filename)
        
    log_utils.print_info_logs("loading parser config file from brand >> market level at: {}". \
                                  format(parser_config_path))
        
    parser_config_file = None
    try:
        parser_config_file = prop_reader.get_config_file_content(const.PARSER_CONFIGURATION_PATH, \
                                        parser_config_path)
    except Exception as e:
        log_utils.print_error_logs("parser config file not found at brand >> market level at: '{}', \
                                    attempting at brand level".format(parser_config_path))
        
    if parser_config_file == None:
        try:
            parser_config_path = "{parser_config_folder}{brand}/{file_name}".format(parser_config_folder=const.PARSER_CONFIGURATION_PATH, \
                    brand=feed_runtime_context.brand, file_name=parser_config_filename)
            
            parser_config_file = prop_reader.get_config_file_content(const.PARSER_CONFIGURATION_PATH, \
                                            parser_config_path, feed_runtime_context)
        except Exception as e:
            raise Exception("ParserConfig file: '{}' not found at brand or market level, error: {}".format(parser_config_filename, str(e)))

    return parser_config_file

def set_data_source_type(market_json_configuration):
    try:
        for _configuration in market_json_configuration["configurations"]:
            feedpathPrefixes = _configuration["feedPathPrefix"].split(",")
            if len(feedpathPrefixes) > 1:
                if(feed_runtime_context.clientType == 'live'):
                    _configuration["feedPathPrefix"] = feedpathPrefixes[0]
                else:
                    _configuration["feedPathPrefix"] = feedpathPrefixes[1]
            else:
                _configuration["feedPathPrefix"] = feedpathPrefixes[0]
        return market_json_configuration
    except Exception as e :
        raise e

def get_current_model(model_name):
    try:        
        absolute_file_path = const.MODEL_PATH + model_name+".json"
        model_json=prop_reader.get_config_prop(
                const.config_section,'read-configuation' , absolute_file_path)
        return model_json
    except Exception as e :
        raise e 

def set_model_data(feed_file_df,file_parser_config_json, model_name, feed_type):
    try:

        parserResult = ParserResult()    
        parserResult.feedType = feed_type
        parserResult.clazz = model_name

        parserResult.parserType = file_parser_config_json["parserType"]

        if("parserType" not in file_parser_config_json):
            raise Exception("Required attribute: 'parserType' missing in: '"+feed_type+"'")

        parserBase: ParserBase = get_parser_by_feed_type(parserResult.parserType)
        if(parserBase == None):
            raise Exception("Parser for type: '{parserType}' not implmented yet".
                format(parserType=parserResult.parserType))

        parserBase.request_context = feed_runtime_context
        parserBase.parser_result = parserResult
        parserBase.data_frame = feed_file_df
        parserBase.parser_config_json = file_parser_config_json

        if(parserBase == None):
            return
            
        sorted_sheets = parserBase.get_sheets()
        
        message = "Total sheets to be processed for type: '{feed_type}' are: {count}".format(
            feed_type=feed_type, count=str(len(sorted_sheets)))

        log_utils.print_info_logs(message)
      
        for index_sheet, sheet in enumerate(sorted_sheets):
            error_message, status = parserBase.parse_sheet(sheet)
            if(status):
                log_utils.print_info_logs(message="Processed "+str(index_sheet+1) +" out of "+ 
                                      str(len(sorted_sheets)) +" sheets of: '"+feed_type+"'")
            else:
                log_utils.print_debug_logs(error_message)
                
        log_utils.print_info_logs(message="Completed parsing of: '"+feed_type+"'")
                                          
        return parserResult
    except Exception as e :
        log_utils.print_error_logs(message="Exception while parsing parserConfiguration of feed type: '"+feed_type+"'")
        traceback.print_exc()
        raise e             

def process_ignite():
    try:
        
        feed_runtime_context = FeedRuntimeContext.get_instance()

        file_name = feed_runtime_context.event_key.split("/")[-1]
        log_utils.print_info_logs(file_name)
        if(not validator.is_valid_file_path()):
            log = "- Ignite Feed file name:{} is invalid.".format(file_name)
            log_utils.add_info_logs(log)
            raise Exception("Ignite Feed file name:{} is invalid.".format(file_name))
        
        log_utils.print_info_logs("Ignite File processing started for : "+feed_runtime_context.brand+const.UNDER_SCORE+feed_runtime_context.country)
        market_config_json = read_config()
        
        if(not validator.validate_ignite_feed_and_find_parser_config(market_config_json)): 
            log_utils.print_error_logs(' - Ignite Feed file name is Invalid!!')
            log = "- Ignite Feed file name:{} is invalid.".format(file_name)
            log_utils.add_info_logs(log)
            raise Exception("Ignite Feed file name:{} is invalid.".format(file_name))

        feed_files_to_process = []
        if len(feed_runtime_context.current_required_feeds_config) > 1:
            feed_runtime_context.supports_multiple_feed_files = True
            if not are_dependent_feeds_exist(feed_runtime_context):
                log_utils.print_info_logs("Terminating glue job current run because required feed files not uploaded on s3")
                feed_runtime_context.is_all_dependet_feed_files_exist_on_s3 = False
                return False

            # support multiple feed files and all feed files are avalible on S3
            feed_runtime_context.is_all_dependet_feed_files_exist_on_s3 = True
            
            for feed_config in feed_runtime_context.current_required_feeds_config:
                name: str = feed_config["filePattern"]
                isOptional: bool = transformer.is_json_has_valid_property(feed_config, "isOptional") \
                                   and feed_config["isOptional"]
                
                fileType: FileType = FileType(name, isOptional)
                feed_runtime_context.dependent_feed_files.append(fileType)

        else:
           # brand market does not supports multiple feed file upload
           # continue to process the feed file for which this glue job is invoked
            feed_runtime_context.is_all_dependet_feed_files_exist_on_s3 = True
            event_key_tokens = feed_runtime_context.event_key.split("/")
            event_originator_feed_file = event_key_tokens[len(event_key_tokens) - 1]
            feed_files_to_process.append(event_originator_feed_file)
            
            for feed_file_to_process in feed_files_to_process:
                name: str = feed_file_to_process
                fileType: FileType = FileType(name, False)
                feed_runtime_context.dependent_feed_files.append(fileType)
            
        process_ignite_feed_files() 

        """
        Job action completed at this point
        """
        feed_runtime_context.etl_status = Status.SUCCESS
        return True
    except Exception as e:        
        raise e 

def are_dependent_feeds_exist(feed_runtime_context: FeedRuntimeContext):

    event_key_tokens = feed_runtime_context.event_key.split("/")
    event_originator_feed_file = event_key_tokens[len(event_key_tokens) - 1]
    
    # create a map to hold two list
    # 1. feed file which are required
    # 2. feed file which are optional
    feed_files_map = {}
    feed_files_map["required"] = []
    feed_files_map["optional"] = []
    
    for config in feed_runtime_context.current_required_feeds_config:
        has_optional = transformer.is_json_has_valid_property(config, "isOptional")
        if has_optional and config["isOptional"]:
            feed_files_map["optional"].append(config["filePattern"])
        else:
            feed_files_map["required"].append(config["filePattern"])    

    remaning_feed_files = list(filter(lambda file: event_originator_feed_file != file, map(
        lambda config: config["filePattern"], feed_runtime_context.current_required_feeds_config)))

    log_utils.print_info_logs("checking S3 for dependent feed files...")

    missing_required_files_on_s3 = s3_utils.check_objects_exists(remaning_feed_files, 
        feed_runtime_context.upload_path, feed_runtime_context)

    # don't count the optional one, take if exist, ignore if not
    missing_required_files_on_s3_exclude_optional =  \
        list(filter(lambda missing_files_on_s3: missing_files_on_s3 not in feed_files_map["optional"], 
                    missing_required_files_on_s3))

    if len(missing_required_files_on_s3_exclude_optional) > 0:
        log_utils.print_info_logs("depended feed files (exluding optional) not exists on s3, "+
                                  "terminate the glue current run, missing files: [{missing_files}]"
                                  .format(missing_files=", ".join(missing_required_files_on_s3_exclude_optional)))
        return False

    # supports multiple feed files and all exists on S3, continue to process all files
    log_utils.print_info_logs("all depended feed files exists on s3, continue with Glue job run")
    
    return True
        
def process_ignite_feed_files():
    feed_runtime_context = FeedRuntimeContext.get_instance()
    
    feed_files_to_process: List[FileType] = feed_runtime_context.dependent_feed_files

    for feed_file_to_process in feed_files_to_process:
        """
        Move files from upload to working directory one by one
        """
        s3_utils_helper.move_file_to_s3(feed_file_to_process)

    if not const.TEST:    
        const.IS_WORKING_FOLDER_EXISTS = True

    try:
        """
        Reading files from working directory one by one
        """
        for feed_file_to_process in feed_files_to_process:
            log_utils.print_info_logs("start processing of feed file:{}".format(feed_file_to_process.name))  
            process_ignite_feed_file(feed_file_to_process)
            log_utils.print_info_logs("completed processing of feed file:{}"
                                        .format(feed_file_to_process.name))
    except Exception as e:
        feed_runtime_context.etl_status = Status.FAILED
        log_utils.print_error_logs("exception while parsing feed file: {}".
                                   format(feed_file_to_process.name)) 
        raise e
    
    """
    Afte parsing all the files, start the next operations, aggregation and all.
    """
    log_utils.print_info_logs("files has been processed, ready for aggregation")
    process()

def process_ignite_feed_file(feed_file_to_process: FileType):
    
    try:
        feed_runtime_context = FeedRuntimeContext.get_instance()
        if feed_runtime_context.is_refresh_feed_context_require():
            refresh_feed_runtime_context(feed_file_to_process, feed_runtime_context)
            
        feed_file_to_process.encoding = feed_runtime_context.encoding
        feed_file_df = s3_utils_helper.read_file_from_s3(feed_file_to_process)
        if feed_file_df == None:
            return

        parser_configs = feed_runtime_context.current_parser_configs

        log_utils.print_info_logs(message="Total parserConfigs are: "+ str(len(parser_configs)))

        try:
            for parser_config_index, parser_config in enumerate(parser_configs):
                feedType = parser_config["config"]
                log_utils.print_info_logs(message="Started parsing of: "+ feedType)
                model_name = parser_config["type"]
                file_parser_config_json = read_parser_configuration( feedType)
                parserResult = set_model_data(feed_file_df, file_parser_config_json, model_name, feedType)
                log_utils.print_info_logs(message="Completed parsing of: "+ feedType)
                log_utils.print_info_logs(message="Completed "+ str(parser_config_index+1) +"/"+ str(len(parser_configs)) +" parser configs")
                feed_runtime_context.parser_results.append(parserResult)
            log_utils.print_info_logs("parsing completed for : {}".format(feedType))
        except Exception as e:
            error_message = ErrorCode.PARSING_FAILED.name.format(error=str(e))
            feed_runtime_context.etl_status = Status.FAILED
            raise EtlException(error_message, ErrorCode.PARSING_FAILED)
                    
    except EtlException as e:
        error_message = str(e.message)
        log_utils.print_error_logs("Unable to process feed file: '{event_key}', error: {error}".format(event_key=feed_runtime_context.event_key, error=error_message))       
        raise e        
    except Exception as e:
        log_utils.print_error_logs("Unable to process feed file: '{event_key}', error: {error}".format(event_key=feed_runtime_context.event_key, error=str(e)))
        raise e
        
def refresh_feed_runtime_context(feed_file_to_process: FileType, feed_runtime_context: FeedRuntimeContext):
    """
    When a brand-market supports multiple feed files upload, the glue job terminate itself, 
    if it does not found all the required feed files.

    If it does find all the required feed files than Glue job starts to excute them(without reinvoking itself).
    The runtime context will still points to the feed file which invoked the glue job 
    which needed to be changed on current processing feed file.
    """
    file_path = feed_runtime_context.event_key
    base_path=file_path.rpartition("/")[0]
    feed_runtime_context.current_file_path = file_path
    feed_runtime_context.upload_path = base_path
    
    feed_runtime_context.event_key = feed_runtime_context.upload_path +"/"+ feed_file_to_process.name
    
    if not validator.validate_ignite_feed_and_find_parser_config(feed_runtime_context.current_market_config):
        raise Exception("parserConfig not avalible")
    

def process():
    feed_runtime_context = FeedRuntimeContext.get_instance()
    try:
        log_utils.print_info_logs("aggregation starts")
        aggregator_utils.apply_aggregation(feed_runtime_context.parser_results)
        log_utils.print_info_logs("aggregation completed")
    except Exception as e:
        error_message = "Exception while applying aggregation business rules: {}".format(str(e))
        etlException = EtlException(error_message, ErrorCode.AGGREGATION_RULE_FAILED, e)
        etlException.detail = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
        raise etlException 

    """
    At this point ETL opeations for all feed files is completed, 
    any opeations related to apigee cache AEM cache, sending status mail need 
    to be done from this point
    """
    log_utils.print_info_logs("aggregation completed")
    try:
        log_utils.print_info_logs("preparing for mongo opeations")
        mongo_helper.prepare_mongo()
        log_utils.print_info_logs("mongo operation completed")
    except RepositoryOperationException as e:
        feed_runtime_context.etl_status = Status.FAILED
        log_utils.print_error_logs(e.message)
        raise Exception(e.message)

    try:
        log_utils.print_info_logs("preparing for cache clear opeations")
        #status = cache_utils_helper.clear_apigee_cache()
        # log_utils.print_info_logs("cache clear opeations completed, status: {}".format(status))
    except CacheClearOpeationException as e:
        log_utils.print_error_logs(e.message)
