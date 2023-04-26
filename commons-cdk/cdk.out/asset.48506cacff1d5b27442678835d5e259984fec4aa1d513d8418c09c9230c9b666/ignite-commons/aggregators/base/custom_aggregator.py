from abc import abstractmethod
from dataclasses import dataclass
from typing import List

from commons.aggregator_result import AggregatorResult

"""
Base class for all the types of aggregations.
"""
@dataclass
class CustomAggregator():

    @abstractmethod
    def aggregate(self, parserResultList, configuration) -> List[AggregatorResult]:
        pass
