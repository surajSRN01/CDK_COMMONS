import random
import uuid
from models import constants as constants
from utilities import log_utils as log_utils
from utilities import validator as validator
from utilities.ignite.feed_runtime_context import FeedRuntimeContext
from aggregators.rules.generics import AbstractGenerateId as abstractGenerateId
from aggregators.rules.aggregator_utils import rules_utils as rules_utils


def apply_rule(results_type_map, configuration):
    feed_runtime_context = FeedRuntimeContext.get_instance()

    try:
        model_ref = feed_runtime_context.current_configuration['name']
        language = configuration["language"] if "language" in configuration else None
        conf_prefix = configuration["prefix"] if "prefix" in configuration else None
        prefix = abstractGenerateId.getPrefix(configuration, language)
        model_name = constants.model_names[model_ref]
        model_parser_list = results_type_map[model_name] if(model_name in results_type_map) else []
        if (len(model_name) > 0):
            for model_parser in model_parser_list:
                for model in model_parser.results:
                    model: model_ref = model
                    igniteId = prefix
                    if (conf_prefix is not None):
                        igniteId = igniteId + conf_prefix
                    # igniteId = igniteId + rules_utils.getOrBlankString(model, "dealerId")
                    # dealer_siteId = rules_utils.getOrDefault(model, "siteId")
                    # if (dealer_siteId is not None):
                    #     igniteId = igniteId + constants.separators["ID_SUFFIX_SEPARATOR"] + dealer_siteId
                    # model["_id"] = igniteId+str(random.randint(0000,99999999))
                    model["_id"] = igniteId+str(uuid.uuid4())
                    model["id"] = igniteId
        return results_type_map
    except Exception as e:
        log_utils.print_error_logs(
            ' - Error while Applying Dealers GenerateDealerID Business Rule!!')
        log_utils.print_error_logs(str(e))
        raise e
  
