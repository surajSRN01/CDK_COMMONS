import json

import models.constants as constants
import utilities.iterator_utils as iterator_utils
import utilities.validator as validator
from aggregators.impl.default_aggregator import DefaultAggregator
from commons.aggregator_result import AggregatorResult
from commons.parser_result import ParserResult
from models.PredicateJoinCondition import PredicateJoinCondition, get_type
import aggregators.rules.default.commons.GenerateBrandCountryLanguageRule as GenerateBrandCountryLanguageRule
from aggregators.rules.default.commons import GenerateIdRule as GenerateIdRule

    
class DefaultIgniteAggregator(DefaultAggregator):

    def __init__(self, results_type_map, configuration):
        self.results_type_map = results_type_map
        self.configuration = configuration
        
    def init_before_business_rules(self):
        self.results_type_map = GenerateIdRule.apply_rule(self.results_type_map, self.configuration)
        return self.results_type_map

    def init_after_business_rules(self):
        self.results_type_map = GenerateBrandCountryLanguageRule.apply_rule(self.results_type_map, self.configuration)
        return self.results_type_map

    def aggregate_result(self, parser_result: ParserResult, clazz: str):
        
        aggregatorResult = AggregatorResult()
        aggregatorResult.parserResult = parser_result
        
        if parser_result.clazz not in constants.collection_names_hint:
            print("Supporting agggrgation '{}', should not reach here".format(parser_result.clazz))
            return None

        aggregatorResult.collection = constants.collection_names_hint[parser_result.clazz]

        if "lifecycles" not in self.configuration:
            return aggregatorResult
        
        life_cycles = list(filter(lambda lc:  "clazz" in lc and lc["clazz"] == clazz, self.configuration["lifecycles"]))

        if validator.is_nan(life_cycles) or iterator_utils.is_empty(life_cycles):
            return aggregatorResult
        
        life_cycle = life_cycles[0]

        if "query" in life_cycle and not validator.is_nan(life_cycle):
            queryObject = json.loads(life_cycle["query"])
            if len(queryObject.keys()) > 0:
                aggregatorResult.lifecycleQuery = life_cycle["query"]

            if "predicate" in life_cycle and not validator.is_nan(life_cycle["predicate"]):
                predicate: PredicateJoinCondition = get_type(life_cycle["predicate"])
                aggregatorResult.predicateJoinCondition = predicate

        return aggregatorResult