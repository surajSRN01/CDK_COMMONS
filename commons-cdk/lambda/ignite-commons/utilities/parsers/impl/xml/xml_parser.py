from commons.sheet_parser_config import SheetParserConfig
from utilities import log_utils as log_utils
from utilities import validator as validator
from utilities.parsers.base.parser_base import ParserBase
from utilities.parsers.commons import sheet_parser as sheet_parser

class XMLParser(ParserBase):

    def __init__(self):
        super()
        self.type = "xml" 

    def get_parser_type(self):
        return self.type

    def parse_sheet(self, sheet):     
        
        result, is_valid = self.validate_sheet_and_get_rows(sheet)
        if is_valid == False:
            error_message = result
            log_utils.print_info_logs(error_message)
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
        return str(field_config["index"])
       
    def validate_sheet_and_get_rows(self, sheet):
        sheet_index_tokens = sheet["index"].split(".")
        rows = self.data_frame
        for index, index_token in enumerate(sheet_index_tokens):
            if not index_token in rows:
                error_message = " sheet: "+str(sheet["index"])+" is empty or not have data"
                log_utils.print_debug_logs(" sheet: "+str(sheet["index"])+" is empty or not have data")
                return error_message, False

            rows = rows[index_token]
        
        return rows, True
    
    def get_value(self, from_object, key):
        
        tokens = key.split(".")
        obj = from_object
        for index, token in enumerate(tokens):
            if(not token in obj or obj[token] == None):
                return None
            if isinstance(obj[token], list):
                values = []
                sourceList = obj[token]
                for source in sourceList:
                    values.append(self.get_value(source, ".".join(tokens[index+1:])))
                return values

            obj = obj[tokens[index]]

        return obj
           

    def get_sheet(self, index):

        if isinstance(self.data_frame, list):
            return self.data_frame

        output = self.data_frame
        path = index.split(".")
        for pathIndex in len(path):
            if isinstance(output, dict):
                output = output[path[pathIndex]]
            elif isinstance(output, list):
                output = output[int(path[pathIndex])]

        return output;  

    def get_sheets(self):
        pdf_sheet_names = list(self.data_frame.keys())        
        sheets = self.parser_config_json["sheets"]
        sorted_sheets = sorted(sheets, key=lambda k: ("priority" not in k, k.get("priority", None)))
        self.sheets_name = pdf_sheet_names
        return sorted_sheets