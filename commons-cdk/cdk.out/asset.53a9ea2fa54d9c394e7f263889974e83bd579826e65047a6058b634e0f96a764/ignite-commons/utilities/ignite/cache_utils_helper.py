import json
from utilities.ignite.feed_runtime_context import FeedRuntimeContext
from exception.cache_clear_operation_exception import CacheClearOpeationException
from utilities import constant as const
from utilities import prop_reader as prop
from utilities import prop_reader as prop_reader
from utilities import validator as validator
from utilities import cache_utils as cache_utils
from utilities import log_utils as log_utils


def clear_apigee_cache(request_context):
    # first get client key
    client_key = get_client_key(request_context)

    # build the autorisation endpoint
    autorisation_endpoint = get_authorization_endpoint(request_context)

    # get the api_key
    api_key = get_api_key(client_key, autorisation_endpoint, request_context)

    # call invalidate cache
    cache_clear_status = call_invalidate_cache(
        client_key, api_key, request_context)

    log_utils.print_info_logs("completed agigee cache clear call", request_context)

    clean_aem_cache(request_context)
    return cache_clear_status


def get_client_key(request_context):
    try:

        client_key_name = "{client_type}.{brand}.client.key.mule".format(client_type=request_context.clientType,
                                                                    brand=request_context.brand)

        client_key = prop.get_prop(
            const.client_key_section, client_key_name, request_context)

        if validator.is_nan(client_key):
            raise CacheClearOpeationException("Client key not avalible for: {} on {}".format(
                client_key_name, request_context.env))
        return client_key
    except CacheClearOpeationException as e:
        raise e
    except Exception as e:
        raise CacheClearOpeationException("Client key not avalible for: {} on {}".format(
            client_key_name, request_context.env))


def get_authorization_endpoint(request_context):
    try:
        autorisation_endpoint = prop.get_prop(
            const.apigee_section, "v2.autorisation.endpoint", request_context)

        enviroment_endpoint = prop.get_prop(
            const.apigee_section, "apigee.url", request_context)

        if validator.is_nan(autorisation_endpoint) or validator.is_nan(enviroment_endpoint):
            raise CacheClearOpeationException(
                "authorization endpoint not found on '{}'".format(request_context.env))

        feed_runtime_context = FeedRuntimeContext.get_instance()
        current_configuration = feed_runtime_context.current_configuration

        language = feed_runtime_context.get_current_language()

        api_path_param = "{country}/{language}".format(country=current_configuration["country"].upper(),
                                                       language=language)

        return "https://{enviroment_endpoint}/{autorisation_endpoint}/{api_path_param}" \
            .format(enviroment_endpoint=enviroment_endpoint, 
                    autorisation_endpoint=autorisation_endpoint,
                    api_path_param=api_path_param)

    except CacheClearOpeationException as e:
        raise e
    except Exception as e:
        raise CacheClearOpeationException(
            "authorization endpoint not found on '{}'".format(request_context.env))


def get_api_key(client_key, authorization_endpoint, request_context):
    try:
        api_key = cache_utils.get_api_key(client_key, authorization_endpoint)
        if validator.is_nan(api_key):
            raise CacheClearOpeationException(
                "Unable to get API key for {}".format(request_context.env))
        return api_key
    except CacheClearOpeationException as e:
        raise e
    except Exception as e:
        raise CacheClearOpeationException(
            "Unable to get API key for {}".format(request_context.env))


def call_invalidate_cache(client_key, api_key, request_context):
    try:
        # get apigee clean cache endpoint url
        apigee_clear_cache_api_endpoint = cache_utils.get_cache_clear_full_endpoint(
            request_context)

        feed_runtime_context = FeedRuntimeContext.get_instance()
        current_configuration = feed_runtime_context.current_configuration
        report = ""

        language = feed_runtime_context.get_current_language()

        for brand in current_configuration["brands"]:

            prefix = brand \
                + const.APIGEE_CLEAR_CACHE_PREFIX_SEPARATOR \
                + current_configuration["country"].upper() \
                + const.APIGEE_CLEAR_CACHE_PREFIX_SEPARATOR \
                + language.upper() \
                + const.APIGEE_CLEAR_CACHE_PREFIX_SEPARATOR \
                + request_context.clientType.lower() \
                + const.APIGEE_CLEAR_CACHE_PREFIX_SEPARATOR

            endpoint = apigee_clear_cache_api_endpoint + prefix

            try:
                status_code = cache_utils.call_invalidate_cache(
                    endpoint, api_key, client_key, request_context)
                report += prefix + " - " + str(status_code) + " ; "
            except Exception as e:
                raise CacheClearOpeationException(
                    "Unable clean apigee cache for {}, reason:{}".format(prefix, str(e)))

        return report

    except CacheClearOpeationException as e:
        raise e
    except Exception as e:
        raise CacheClearOpeationException(
            "Unable to get API key for {}".format(request_context.env))

def clean_aem_cache(request_context):

    try:
        lambda_enabled_markets = prop.get_prop(
            const.apigee_section, "lambda.enable.marktes", request_context)

        country = request_context.country.upper()
        if country not in lambda_enabled_markets.split(","):
            return

        topic_arn = prop.get_prop(const.sns_section, "topic.arn", request_context)
        sns_region = prop.get_prop(const.sns_section, "sns.regions", request_context)
        environment = prop.get_prop(const.sns_section, "environment", request_context)

        message_body = {
            "cacheclear" : {
                "Brand": request_context.brand,
                "Market" : request_context.country,
                "Environment" : sns_region,
                "FeedType" : "Dealer",
                "Site": request_context.clientType
            }
        }

        subject = "Dealer cache clear request"
        log_utils.print_info_logs("Sending ----- {} to sns".format(json.dumps(message_body)), request_context)
       
        cache_utils.publish_sns_notification(topic_arn, message_body, subject, request_context)

        log_utils.print_info_logs("completed sending message to sns -----------------------------", request_context)
    except Exception as e:
        log_utils.print_error_logs("unbale to send SNS notification reason:  {}".format(str(e)), request_context)