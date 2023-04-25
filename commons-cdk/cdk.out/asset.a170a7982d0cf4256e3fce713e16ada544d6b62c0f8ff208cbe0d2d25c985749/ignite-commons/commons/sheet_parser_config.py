from dataclasses import dataclass
from enum import Enum

from commons.parser_result import ParserResult
from utilities import validator as validator
from utilities.parsers.base.parser_base import ParserBase


class SheetJoinType(Enum):
    NO_JOIN = "no join column on sheet"
    HAS_JOIN_BUT_NO_TARGET_LIST_PATH = "has join column and but no target list path on sheet"
    HAS_JOIN_AND_TARGET_LIST_PATH = "has join column and has target list path on sheet"

class SheetParsingStatus:
    SUCCESS = "SUCCESS",
    FAILED = "FAILED"
    INPROGRESS = "INPROGRESS"

@dataclass
class SheetParserConfig:

    sheet: dict
    row: None
    row_index: int
    parser_result: ParserResult
    status: SheetParsingStatus
    error_message: str
    request_context: None
    sheet_join_type: SheetJoinType
    parser: ParserBase

    def __init__(self, sheet, parser_result, request_context):
        self.sheet = sheet
        self.parser_result = parser_result
        self.request_context = request_context
        self.status = SheetParsingStatus.INPROGRESS
        self.sheet_join_type = self.get_sheet_join_type()
        
    def mark_success(self):
        self.status = SheetParsingStatus.SUCCESS

    def mark_failed(self, error_message):
        self.status = SheetParsingStatus.FAILED
        self.error_message = error_message

    """
    Function to get what type of Join does a sheet have
    """
    def get_sheet_join_type(self):
        sheet_config = self.sheet
        # Case 1. sheet neither has joinBy nor tagetListPath, simply the join attribute 
        # not exist on sheet
        if not("join" in sheet_config.keys()):
            return SheetJoinType.NO_JOIN

        # Case 2. sheet has join, neither has joinBy nor tagetListPath
        has_target_list_path = "targetListPath" in sheet_config["join"]
        if(has_target_list_path == False or validator.is_nan(sheet_config["join"]["targetListPath"])):
            return SheetJoinType.HAS_JOIN_BUT_NO_TARGET_LIST_PATH

        # Case 3. sheet has join and tagetListPath
        return SheetJoinType.HAS_JOIN_AND_TARGET_LIST_PATH            