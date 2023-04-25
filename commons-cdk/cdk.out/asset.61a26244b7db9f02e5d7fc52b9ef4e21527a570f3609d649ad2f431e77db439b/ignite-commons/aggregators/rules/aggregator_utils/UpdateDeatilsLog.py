import re
from typing import List

from commons.aggregator_result import AggregatorResult


class UpdateDetailLog:
	
	PASSED = 0
	FAILED = 1
	FIRST_INDEX = 0
	PATTERN_NUMERIC = "(\\d+)"
	MESSAGE_DETAIL = "Passed : '{}'. Failed : '{}'"
	
	def validar(detail, n):
		pattern = re.compile(UpdateDetailLog.PATTERN_NUMERIC)
		matches: List[str]= pattern.findall(detail)
		total_passed = n 
		total_failed = (int(matches[1]) + int(matches[0])) - n
		
		return UpdateDetailLog.MESSAGE_DETAIL.format(str(total_passed), str(total_failed))
	
	def updateLog(aggregatorResult: AggregatorResult):
		for log_index, log in enumerate(aggregatorResult.parserResult.report.logs):
			for feed_index, feed_type in enumerate(aggregatorResult.parserResult.report.feed_types):
				log: str = log
				if log.__contains__(feed_type):
					log_with_counter = aggregatorResult.parserResult.report.logs[log_index+1]
					log_with_counter_reset = UpdateDetailLog.validar(log_with_counter, aggregatorResult.parserResult.report.failed)
					aggregatorResult.parserResult.report.logs[log_index] = log_with_counter_reset
					break