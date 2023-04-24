from aggregators.base.custom_aggregator import CustomAggregator
from utilities import log_utils as log_utils

class AbstractAggregator(CustomAggregator):
 
    def group_results_by_type(self, parser_result_list):
        typeResults = {}
        for resultList in  parser_result_list:
            clazz = self.getTypeByConfigFileName(resultList.feedType)
            if clazz in typeResults.keys():
                list = typeResults.get(clazz)
                list.append(resultList)
            else:
                list = []
                list.append(resultList)
                typeResults[clazz] = list
                
        return typeResults
        

    def aggregate(self, parserResultList):
    
        results_group_by_type = self.group_results_by_type(parserResultList)
        
        self.results_type_map = results_group_by_type
        log_utils.print_info_logs("starting to apply before-business rules")
        self.results_type_map = self.init_before_business_rules()
        log_utils.print_info_logs("completed before-business rules")

        # apply after rules on merged results
        log_utils.print_info_logs("started merging of results set")
        merged_parsed_results = self.merge_results()
        log_utils.print_info_logs("completed merging of results set")
        
        self.results_type_map = merged_parsed_results
        log_utils.print_info_logs("starting to apply after-business rules")
        self.init_after_business_rules()
        log_utils.print_info_logs("completed after-business rules")
        # self.remove_duplicates()
        
        return self.convertMapToList(self.results_type_map)

    def convertMapToList(self, map):
        aggregator_results_list = []
        for key in map.keys():
            for parserResult in map.get(key):
                aggregation_result = self.aggregate_result(parserResult, key)
                if  aggregation_result is not None:
                    aggregator_results_list.append(aggregation_result)

        return aggregator_results_list

    def getTypeByConfigFileName(self, configFileName):
        clazz = None
        for requiredFeed in self.configuration["requiredFeeds"]:
            for parserConfiguration in requiredFeed["parserConfigurations"]:
                if parserConfiguration["config"] == configFileName:
                    clazz = parserConfiguration["type"]
        
            if clazz != None:
                break
            
        return clazz
          