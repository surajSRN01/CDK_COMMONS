from enum import Enum
from typing import List
from commons.parser_result import ParserResult
from models.PredicateJoinCondition import PredicateJoinCondition

class Status(Enum):
    SUCCESS = "success",
    FAIELD = "failed"

class AggregatorResult():

    route: str
    lifecycleQuery: str
    parserResult: ParserResult
    collection: str
    status: Status
    error_message: str
    predicateJoinCondaition: PredicateJoinCondition
    feed_types: List[str]

    def __init__(self) -> None:
        self.route = ""
        self.lifecycleQuery = ""
        self.parserResult = None
        self.collection = ""
        self.status = None
        self.error_message = None
        self.predicateJoinCondition = PredicateJoinCondition.AND