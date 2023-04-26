import pandas as pd
import json
from commons.sheet_parser_config import SheetParserConfig
from jsonpath_ng import jsonpath, parse
from utilities import dataframework_utils as df_utils
from utilities import log_utils as log_utils
from utilities import validator as validator
from utilities.parsers.commons import sheet_parser as sheet_parser
from utilities.parsers.impl.xml.xml_parser import XMLParser

class XMLPathParser(XMLParser):

    def __init__(self):
        super()
        self.type = "xmlpath" 

    def get_parser_type(self):
        return self.type

    def parse_sheet(self, sheet):     
        
        result, is_valid = self.validate_sheet_and_get_rows(sheet)

        if is_valid == False:
            error_message = result
            log_utils.print_info_logs(error_message, self.request_context)
            return error_message, False
        
        rows = result
        sheet_parser_config = SheetParserConfig(sheet=sheet, parser_result = self.parser_result, 
                            request_context = self.request_context)

        sheet_parser_config.parser = self  

        for row_index, row in enumerate(rows, start = sheet["skip"]):
            sheet_parser_config.row = row
            sheet_parser_config.row_index = row_index
            sheet_parser.parser_sheet(sheet_parser_config)
   
        return "", False
               
    def validate_sheet_and_get_rows(self, sheet):
        sheet_index = sheet["index"]
        skip = sheet["skip"]
        sheet_index = sheet_index.replace("*", "[*]")
        jsonpath_expr = parse(sheet_index)
        rows = jsonpath_expr.find(self.data_frame)
        rows = list(map(lambda row: row.value, rows))
        """
        TODO: Add validator.remove_row_column_nan for xml
        current_sheet_pdf = validator.remove_row_column_nan(current_sheet_pdf, self.request_context)
        """
        if len(rows) <= int(skip):
            error_message = "sheet: "+ str(sheet_index)+" is empty, skipping"
            return error_message, False  

        return rows, True