from enum import Enum


class PredicateJoinCondition(Enum):

    AND = "and"
    OR = "or"

def get_type(name):
    if PredicateJoinCondition.AND.value == name:
        return  PredicateJoinCondition.AND

    if PredicateJoinCondition.OR.value == name:
        return  PredicateJoinCondition.OR

    # Should thrown exception if something else provided