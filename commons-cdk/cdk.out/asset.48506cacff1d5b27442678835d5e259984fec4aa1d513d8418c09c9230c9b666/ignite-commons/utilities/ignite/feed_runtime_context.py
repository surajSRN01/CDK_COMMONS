"""
FeedRuntimeContext, a singleton class to expose only one instance, related to
the current feed file that is currently being by the Glue Job.
"""
from enum import Enum
from typing import List

from models.FileType import FileType
from utilities.model.singleton import Singleton

class Status(Enum):
    SUCCESS = "success",
    FAILED = "fail"

class JobStatus(Enum):
    SUCCESS = "success",
    FAILED = "fail"    

@Singleton
class FeedRuntimeContext():
    
    # object properties
    current_file_path: str
    supports_multiple_feed_files: bool
    is_all_dependet_feed_files_exist_on_s3: bool
    dependent_feed_files: List[FileType]
    feed_file_parser_results_map = None
    
    current_market_config:None
    # selected/matched configuration from configurations of current_market_config 
    current_configuration: None
    # selected/matched requiredFeeds from current_configuration 
    current_required_feeds_config: None
    # parserConfigurations from current_required_feeds_config
    current_parser_configs: None
    
    upload_path: str
    
    encoding: str
    runtime_context: None
    feed_file_aggregation_results_map = None

    # use this flag to determine where ETL process completed or failed, 
    # regardless of dependent or no dependent feed files 
    etl_status: Status
    
    log_file_path: str
    
     # hold parser results at a single place
    parser_results = []

    has_consolidate_log_file: bool
    brand_country_language_prefix = None
    
    request_context: None

    jobStatus: JobStatus
    stackTrace: None

    def __init__(self) -> None:
        # set all the default properties here
        self.current_file_path = ""
        self.supports_multiple_feed_files = False
        self.dependent_feed_files = []
        self.feed_file_parser_results_map = {}
        self.is_all_dependet_feed_files_exist_on_s3 = False
        self.runtime_context = None
        self.upload_path = ""
        self.current_parser_configs = None
        self.current_configuration = None
        self.current_required_feeds_config = None
        self.encoding = ""
        self.current_market_config = None
        self.feed_file_aggregation_results_map = {}
        self.etl_status = None
        self.log_file_path = None
        self.mongo_client = None
        self.optional_feed_files = []
        self.is_optional_feed_file = False
        self.parser_results = []
        self.has_consolidate_log_file = False
        self.brand_country_language_prefix = None
        self.request_context = None
        # default success, change the status from SUCCESS to Failed
        # if any exception came
        self.jobStatus = JobStatus.SUCCESS
        self.stackTrace = None 

    def terminate_job_silently(self):
        """
        A run of Glue job need to be termiate silently when the current processing feed has
        dependent feed files and all dependent feed files are not yet uploaded on desired S3 folder. 
        If the current running market depended upon additional feed files and all feed files
        are not exists on s3 then the current run should siletly termiate, means leaving
        the current feed file on upload folder.
        """
        return self.supports_multiple_feed_files and not self.is_all_dependet_feed_files_exist_on_s3     

    def get_dependent_feed_files_as_event_key(self) -> List[FileType]:
        feed_files_as_event_keys:List[FileType] = []
        upload_path = self.upload_path
        for feed_file in self.dependent_feed_files:
            file = FileType(upload_path +"/"+feed_file.name,feed_file.isOptional)       
            feed_files_as_event_keys.append(file)
        
        return feed_files_as_event_keys

    def is_refresh_feed_context_require(self):
        if not self.supports_multiple_feed_files:
            return False

        return self.is_all_dependet_feed_files_exist_on_s3


    def get_current_language(self):     
        current_configuration = self.current_configuration
        language = None    
        invalid_values = (None, "", "nan")
        if "language" in current_configuration and current_configuration["language"] not in invalid_values:
            language = current_configuration["language"]

        if language == None:
            for key in self.feed_file_aggregation_results_map.keys():
                result = self.feed_file_aggregation_results_map.get(
                    key)
                language = result[0].parserResult.results[0].language
                break
        return language