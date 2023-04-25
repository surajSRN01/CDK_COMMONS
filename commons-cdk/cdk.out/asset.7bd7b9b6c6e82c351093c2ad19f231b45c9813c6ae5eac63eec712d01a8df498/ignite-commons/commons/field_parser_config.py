from dataclasses import dataclass

from commons.list_type import ListType
from commons.sheet_parser_config import SheetParserConfig
from utilities.parsers.base.parser_base import ParserBase


class FieldParsingStatus:
    SUCCESS = "SUCCESS",
    FAILED = "FAILED"
    INPROGRESS = "INPROGRESS"

@dataclass
class FieldParserConfig:

    model: object
    sheet_index: any
    row: object
    row_index: int
    fields: list
    field_config_length: int
    current_field_config_index: int
    request_context: object
    status: FieldParsingStatus
    error_message: str
    parser: ParserBase

    # to enble the travel bi-directional
    parent_field_parser_config: None
    child_class_type: None
    current_list_type: ListType
    callRecursive: bool
    ignoreException: bool

    def __init__(self, model, sheet_index, row, row_index, fields, current_field_config_index, request_context):
        self.model = model
        self.sheet_index = sheet_index
        self.row = row
        self.row_index = row_index
        self.fields = fields
        self.field_config_length = len(fields)
        self.current_field_config_index = current_field_config_index
        self.request_context = request_context
        self.status = FieldParsingStatus.INPROGRESS
        self.parent_field_parser_config  = None
        self.error_message = ''
        self.current_list_type = None
        self.child_class_type = None
        self.callRecursive = True
        self.ignoreException = False

    def create_field_parser_config_from_sheet_parser_config(target_object, sheetParserConfig: SheetParserConfig):
        sheet_config = sheetParserConfig.sheet
        field_config = FieldParserConfig(model=target_object, sheet_index=sheet_config["index"], 
                                row=sheetParserConfig.row, row_index=sheetParserConfig.row_index, 
                                fields=sheet_config["fields"], current_field_config_index=0,
                                request_context=sheetParserConfig.request_context)
        field_config.parser = sheetParserConfig.parser
        return field_config                         

    def clone(self, model, fields_config):
        field_parser_config = FieldParserConfig(self.model, self.sheet_index, self.row, self.row_index, self.fields,
                                                self.current_field_config_index, self.request_context)

        field_parser_config.fields = fields_config
        field_parser_config.field_config_length = len(fields_config)
        field_parser_config.current_field_config_index = 0
        field_parser_config.model = model
        field_parser_config.parent_field_parser_config = self
        field_parser_config.parser = self.parser
        field_parser_config.current_list_type = self.current_list_type
        field_parser_config.child_class_type = self.child_class_type

        return field_parser_config

    def mark_success(self):
        self.status = FieldParsingStatus.SUCCESS

    def mark_failed(self, error_message):
        self.status = FieldParsingStatus.FAILED
        self.error_message = error_message
        self.parser.parser_result.report.add_failed(error_message)

    def is_completed(self):
        # a fields config iteration will be completed when it follow below conditions
        # all field configs are iterated
        is_all_field_config_covered = self.current_field_config_index == self.field_config_length
        # does not have parent
        previous = self.parent_field_parser_config
        # current status is in-progress
        in_progress = self.status == FieldParsingStatus.INPROGRESS
        return is_all_field_config_covered and in_progress and previous == None
            