
from aggregators.impl.default_ignite_aggregator import DefaultIgniteAggregator


class BrandJpAggregator(DefaultIgniteAggregator):

    def __init__(self, results_type_map, configuration):
        self.results_type_map = results_type_map
        self.configuration = configuration

    def init_before_business_rules(self):

        self.results_type_map = super().init_before_business_rules()

        return self.results_type_map

    def init_after_business_rules(self):
        self.results_type_map = super().init_after_business_rules()
        return self.results_type_map

