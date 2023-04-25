from models import constants as constants
from utilities import log_utils as log_utils
from utilities import validator as validator
from utilities.ignite.feed_runtime_context import FeedRuntimeContext


def apply_rule(results_type_map, configuration):
    feed_runtime_context = FeedRuntimeContext.get_instance()

    try:
        model = feed_runtime_context.current_configuration['name']
        parser_results_list = results_type_map[model] if(model in results_type_map) else []
        if len(parser_results_list) > 0:
            for parser_result_list in parser_results_list:
                for result in parser_result_list.results:
                    result.brands = configuration["brands"]
                    result.owner = configuration["owner"]
                    result.country = configuration["country"]
                    if "language" in configuration and not validator.is_nan(configuration["language"]):
                        result.language = configuration["language"]
                        
        return results_type_map
    except Exception as e :
        raise e
