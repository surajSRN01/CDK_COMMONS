from exception.error_code import ErrorCode
from exception.etl_exception import EtlException
import utilities.dataframework_utils as df_utils
import utilities.log_utils as log_utils
import utilities.parsers.commons.sheet_parser as sheet_parser
import utilities.validator as validator
from commons.sheet_parser_config import SheetParserConfig
from utilities.parsers.base.parser_base import ParserBase


class ExcelParser(ParserBase):

    def __init__(self):
        super()
        self.type = "excel"

    def get_parser_type(self):
        return self.type

    def parse_sheet(self, sheet):
    
        validation_result, is_valid = self.validate_sheet_and_get_data_frame(sheet)        
        
        if is_valid == False:
            error_message = validation_result
            log_utils.print_info_logs(error_message, self.request_context)
            return error_message, False

        data_frame = validation_result
        sdf = df_utils.pandas_to_spark(data_frame, self.request_context)
        rows = sdf.collect()
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
   
    def validate_sheet_and_get_data_frame(self, sheet):

        sheet_index= int(sheet["index"])
        skip = sheet["skip"]

        current_sheet_pdf = self.data_frame[self.sheets_name[sheet_index]]
        current_sheet_pdf = validator.remove_row_column_nan(current_sheet_pdf)
        data_row_count = len(current_sheet_pdf.index)

        if data_row_count < int(skip) and sheet_index==0:
            raise EtlException("sheet index 0 is empty",ErrorCode.REQUIRED_FEED_FILE_MISSING)

        mandatory_columns=[]     
        validator.get_mandatory_columns(sheet["fields"],mandatory_columns)
        
        if len(mandatory_columns) > 0:
            if  len(current_sheet_pdf.columns) < len(mandatory_columns):
                return "Sheet: "+str(sheet["index"])+". >> Wrong number of columns received", False    
        
        return current_sheet_pdf, True

    def get_value(self, from_object, key):
        if int(key) > len(from_object):
            return None
        return from_object[int(key)]

    def get_sheets(self):
        pdf_sheet_names = list(self.data_frame.keys())        
        sheets = self.parser_config_json["sheets"]        
        sorted_sheets = sorted(sheets,key=lambda k:k["priority"])
        self.sheets_name = pdf_sheet_names
        return sorted_sheets    