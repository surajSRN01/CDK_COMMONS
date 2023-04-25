
from io import StringIO

import pandas as pd
from commons.sheet_parser_config import SheetParserConfig
from models.FileType import FileType
from utilities import constant as constant
from utilities import dataframework_utils as df_utils
from utilities import log_utils as log_utils
from utilities import validator as validator
from utilities.ignite.feed_runtime_context import FeedRuntimeContext
from utilities.parsers.base.parser_base import ParserBase
from utilities.parsers.commons import sheet_parser as sheet_parser


class CsvParser(ParserBase):

    def __init__(self):
        super()
        self.type = "csv"

    def get_parser_type(self):
        return self.type

    def parse_sheet(self, sheet):
        result, is_valid = self.validate_sheet_and_get_data_frame(sheet)        
        
        if is_valid == False:
            error_message = result
            log_utils.print_info_logs(error_message, self.request_context)
            self.parser_result.report.add_failed(error_message)
            return error_message, False
        
        rows = result
        sheet_parser_config = SheetParserConfig(sheet=sheet, parser_result = self.parser_result, request_context = self.request_context)
        sheet_parser_config.parser = self
        sheet_parser_config.parser_result.report.total = len(rows)
        
        for row_index, row in enumerate(rows, start = sheet["skip"]):
            sheet_parser_config.row = row
            sheet_parser_config.row_index = row_index
            sheet_parser.parser_sheet(sheet_parser_config)    
        
        return "", True

    def get_source_field(self, field_config):
        return int(field_config["index"])

    def validate_sheet_and_get_data_frame(self, parser_config_json):
        feed_runtime_context = FeedRuntimeContext.get_instance()
        file_path = self.request_context.event_key
        file_name = file_path.split("/")[-1]
        
        file_name_lambda = filter(lambda file_type: file_type.name == file_name, feed_runtime_context.dependent_feed_files)
        file_types = list(file_name_lambda)
        file_type: FileType = file_types[0] if len(file_types) > 0 else None
        
        encoding = None
        if file_type is not None and hasattr(file_type, "encoding") and not validator.is_nan(file_type.encoding):
            encoding = file_type.encoding
        
        # by default quote mark avalible
        quoteMark = None 
        if "quoteMark" in self.parser_config_json:
            quoteMark = self.parser_config_json["quoteMark"]

        # default csv separator, take from sheet config if avalible
        separator = ","
        if "separator" in self.parser_config_json:
            separator = self.parser_config_json["separator"]

        if encoding is None:
            encoding = ","
            
        data = self.convert_byte_to_string(encoding)

        if data == None: 
            raise Exception("Unable to read file from encoding: [{}]".format(encoding.split("/")))
        
        df = None
        if quoteMark is not None:
            df = pd.read_csv(data, sep=separator, quotechar=quoteMark, dtype=str)
        else:
            df = pd.read_csv(data, sep=separator, dtype=str)    

        sdf = df_utils.pandas_to_spark(df, self.request_context)
        skip = self.parser_config_json["sheets"][0]["skip"]
        rows = sdf.collect()
        data_row_count = len(rows)

        if data_row_count <= int(skip):
            return "sheet: "+ str(0)+" is empty, skipping", False  

        mandatory_columns=[]     
        validator.get_mandatory_columns(self.get_sheets()[0]["fields"], mandatory_columns)
        
        total_columns = len(sdf.columns)

        if len(mandatory_columns) > 0:
            if total_columns  < len(mandatory_columns):
                return "Sheet: "+str(self.get_sheets()[0]["index"])+". >> Wrong number of columns received", False    

        return rows, True

    def get_value(self, from_object, key):
        if int(key) > len(from_object):
            return None
        return from_object[int(key)]

    def get_sheets(self):
        # append index, csv sheet config don't have index
        self.parser_config_json["sheets"][0]["index"] = 0
        # always take the first sheet, csv don't have sheets
        sheet = self.parser_config_json["sheets"][0]       
        pdf_sheet_names = ["(index:0)"]
        sheets = []
        sheets.append(sheet)
        sorted_sheets = sorted(sheets,key=lambda k:k["priority"])
        self.sheets_name = pdf_sheet_names
        return sorted_sheets
    
    def convert_byte_to_string(self, encoding: str):
        if "," in encoding:
            encodings = encoding.split(",")
            encodings.append(constant.DEFAULT_ENCODING)
            for enc in encodings:
                try:
                    return StringIO(str(self.data_frame, enc))
                except Exception as e:
                    log_utils.print_debug_logs("unable to read file from encoding:{}".format(enc))
                    continue

        return StringIO(str(self.data_frame, constant.DEFAULT_ENCODING))