from abc import abstractmethod
from dataclasses import dataclass
from commons.parser_result import ParserResult
from utilities import log_utils as log_utils
from utilities import validator as validator

"""
Base class for all the types of parsers.
"""
@dataclass
class ParserBase():

    request_context: None
    parser_result: ParserResult
    data_frame: None
    sheets_name: None
    parser_config_json: None

    @abstractmethod
    def get_value(self, from_object, key) -> str:
        pass

    @abstractmethod
    def get_parser_type(self):
        pass

    @abstractmethod
    def parse_sheet(self, sheet):
        pass

    @abstractmethod
    def get_source_field(self, field_config):
        pass
    
    @abstractmethod
    def get_sheets(self):
        pass     
    
    def __getitem__(self, item):
        return getattr(self, item)   

    def __setitem__(self, item, value):
        return setattr(self, item, value)      