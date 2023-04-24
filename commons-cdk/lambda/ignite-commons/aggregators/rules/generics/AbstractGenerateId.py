from utilities import log_utils as log_utils
from models import constants as constants
from utilities.ignite.feed_runtime_context import FeedRuntimeContext

def getPrefix(configuration,language) :
    try:
        prefix = configuration["country"] + constants.separators["ID_SEPARATOR"]
        if ("multilanguage" in configuration and configuration["multilanguage"] == True) :
            if language is not None:
                prefix  = prefix + language + constants.separators["ID_SEPARATOR"]
            else:
                prefix  = prefix + constants.separators["ID_SEPARATOR"]
        prefix += configuration["owner"] + constants.separators["ID_SEPARATOR"]
        return prefix
    except Exception as e :
        log_utils.print_error_logs(' - Error while invoking getPrefix Method!!')
        log_utils.print_error_logs(str(e))
        raise e
