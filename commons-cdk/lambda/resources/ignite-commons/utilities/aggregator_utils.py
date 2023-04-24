from typing import List
import aggregators.init_ignite_aggregator as init_ignite_aggregator
import utilities.constant as const
import utilities.log_utils as log_utils
import utilities.validator as validator
import utilities.iterator_utils as iterator_utils
from aggregators.base.custom_aggregator import CustomAggregator
from commons.parser_report import ParserReport
from commons.parser_result import ParserResult
from utilities.ignite.feed_runtime_context import FeedRuntimeContext


def merge_reports(destination: ParserReport, source: ParserResult):
    
    destination.logs.append("### Feed : {feed_type}".format(feed_type=source.feedType)) 
    if source is not None:
        destination.feed_types.append({"feedType": source.feedType, "total": source.report.total})
        destination.failed = source.report.failed
        destination.passed = source.report.passed
        destination.logs.append("Passed : {passed}, Failed: {failed}".format(passed=destination.passed, failed=destination.failed))
        destination.logs.append("")
        destination.logs.extend(source.report.logs)

    destination.logs.append("")
    destination.logs.append("")
               
def apply_aggregation(parser_results_list):
    feed_runtime_context = FeedRuntimeContext.get_instance()
    try:
        classmap = init_ignite_aggregator.classmap
        # find aggregator concreet class
        country=feed_runtime_context.country
        aggegator_classname = feed_runtime_context.brand + const.UNDER_SCORE + country.replace("-","") + "_aggregator"
        aggegator_class = classmap[aggegator_classname] if aggegator_classname in classmap else classmap[feed_runtime_context.brand+'_default_aggregator']

        if(validator.is_nan(aggegator_class)):
            log_utils.print_error_logs("No aggregator defined for brand:{brand} market: {market}".format(brand=feed_runtime_context.brand, market=feed_runtime_context.country))
            return Exception("No aggregator defined for brand:{brand} market: {market}".format(brand=feed_runtime_context.brand, market=feed_runtime_context.country))


        # create instance of aggregator concreet class     
        aggegator_instance: CustomAggregator = aggegator_class(None, feed_runtime_context.current_configuration)        

        # apply aggregation rules
        aggregated_results = aggegator_instance.aggregate(parser_results_list)        
        
        # store aggregation result against the feed file    
        result_map_entry = {feed_runtime_context.current_file_path : aggregated_results}
        feed_runtime_context.feed_file_aggregation_results_map.update(result_map_entry)

    except Exception as e:
        log_utils.print_error_logs("Error while applying business for: '{}'".format(feed_runtime_context.event_key)) 
        raise e

def get_parser_result_by_feed( parser_results, config):
    try:
        parserResult: ParserResult = None 
        if len(parser_results) > 0:
            for parser_result in parser_results:
                if isinstance(config, list) and parser_result["feedType"] in config:
                    parserResult = parser_result
                    break    
                else:
                    if parser_result["feedType"] == config:
                        parserResult = parser_result
                        break
        return parserResult
    except Exception as e :        
        log_utils.print_error_logs(str(e))
        raise e    

def add_logs(report: ParserReport, logs:list):
    report.logs.extend(logs)
    decrement_successful(report, len(logs))

def decrement_successful(report: ParserReport , failed: int):
    report.failed = report.failed + failed
    report.passed = report.passed - failed    

def removeParserResultByFeed(resultsByType, resultsClass, configName):
    """
    Removes the {@link ParserResult} corresponding to the configuration <b>configName</b>, from the list of parser
    results. If there is no other parserResult for the specified class, than the class is removed from
    <b>resultsByType</b> map.

    @param resultsByType the map of parser results
    @param resultsClass  the class which is to be removed
    @param configName    the name of the config whose parser result is to be removed
    @return {@link ParserResult} corresponding to the configuration <b>configName</b>.
    """
    if resultsClass not in resultsByType:
        return
     
    parserResults:List[ParserResult] = resultsByType[resultsClass]

    if parserResults == None or iterator_utils.is_empty(parserResults):
        return
    
    for parserResult in parserResults[:]:
        parserResult:ParserResult = parserResult 
        
        inCollection = isinstance(configName, list) and parserResult.feedType in configName
        eqMatch = (not isinstance(configName, list)) and parserResult.feedType == configName

        if inCollection or eqMatch:
            parserResults.remove(parserResult)
            if (len(parserResults) == 0):
                del resultsByType[resultsClass] 