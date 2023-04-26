
from aggregators.impl.default_ignite_aggregator import DefaultIgniteAggregator
from models import constants as constants
from utilities import log_utils as log_utils
from utilities import validator as validator


class BrandUsAggregator(DefaultIgniteAggregator):

    
    def __init__(self, results_type_map, configuration):
        self.results_type_map = results_type_map
        self.configuration = configuration

    def init_before_business_rules(self):
        
        rules = []

        for rule in rules:
            self.results_type_map= rule.apply_rule(self.results_type_map, self.configuration)

        self.results_type_map = super().init_before_business_rules()
        return self.results_type_map
    
    def init_after_business_rules(self):

        self.results_type_map = super().init_after_business_rules()

        rules = []
        for rule in rules:
            self.results_type_map = rule.apply_rule(self.results_type_map, self.configuration)
        
        return self.results_type_map 
    