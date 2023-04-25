
from aggregators.base.abstract_aggregator import AbstractAggregator
import models.constants as constants
import models.model_map as models_map
# import utilities.aggregator_utils as aggregator_utils
from utilities import aggregator_utils as aggregator_utils
from commons.parser_report import ParserReport
from commons.parser_result import ParserResult
from utilities.bean_utils import BeanUtils
from utilities import log_utils as log_utils


class DefaultAggregator(AbstractAggregator):

    def merge_results(self):
        try:
            parser_results_by_type = self.results_type_map
            mergedResults = {}
            for clazz in parser_results_by_type.keys():
                entity = models_map.get_model_class_by_config(clazz)
                beanUtils = BeanUtils(
                    entity, constants.class_merge_attributes_map.get(clazz), True, False, True)

                parser_report = ParserReport()
                parser_report.logs = []

                parser_result = ParserResult()
                parser_result.clazz = clazz
                parser_result.report = parser_report
                resultsListsList = []
                
                for parser_result_by_type in parser_results_by_type.get(clazz):
                    resultsListsList.append(parser_result_by_type.results)
                    aggregator_utils.merge_reports(
                        parser_result.report, parser_result_by_type)

                parser_result.results = beanUtils.merge_objects_list(
                    resultsListsList)

                mergedResults[clazz] = []
                mergedResults[clazz].append(parser_result)
            
            return mergedResults
        except Exception as e:
            log_utils.print_error_logs(
                " error while merging results after applying inti before business rules ", self.request_context)
            raise e
        
        
    def remove_duplicates(self):
        try:
            for clazz in self.results_type_map.keys():
                if(clazz == 'com.digitaslbi.pace.model.serviceV2.Service'):
                    continue
                parser_list = self.results_type_map[clazz] if (clazz in self.results_type_map) else []
                
                for parse in parser_list:
                    unique_value = []
                    for valueobject in parse.results[:]:
                        valueobject: object = valueobject
                        
                        if valueobject[valueobject.UNIQUE_BY] in unique_value:
                            parse.results.remove(valueobject)
                            continue
                        unique_value.append(valueobject[valueobject.UNIQUE_BY])

        except Exception as e:
            log_utils.print_error_logs(
                ' - Error while Applying RemoveDuplicates !!')
            log_utils.print_error_logs(str(e))
            raise e