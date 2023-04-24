from utilities.parsers.impl.excel.excel_parser import ExcelParser
from utilities.parsers.impl.xml.xml_parser import XMLParser
from utilities.parsers.impl.csv.csv_parser import CsvParser
from utilities.parsers.impl.xml.xml_path_parser import XMLPathParser
from utilities.parsers.impl.fixed.fixed_path_parser import FixedPathParser

"""
Get correct type of Parser based upon provided feed file type
"""
def get_parser_by_feed_type(type: str):
    if(type == "xlsx"):
        return ExcelParser()
    if(type ==  "xml"):
        return XMLParser()
    if(type ==  "csv" or type == "txt"):
        return CsvParser()   
    if(type ==  "xmlpath"):
        return XMLPathParser()
    if(type ==  "fixed"):
        return FixedPathParser()
    
    raise Exception("Parser for feed type:{} not implemented".format(type))   
