from enum import Enum

class ErrorCode(Enum):
    REQUIRED_FEED_FILE_MISSING = "Required feed file [{files}] missing"
    MISSING_REQUIRED_COLUMN = "Value for required column: '{column}' missing on row: {row_index} on sheet: {sheet_index}"
    AGGREGATION_RULE_FAILED = "Rule: {rule_name} failed to apply"
    PARSING_FAILED = "Parsing failed, error: {error}"
    IGNORE_ABLE = "Ignore able exception"