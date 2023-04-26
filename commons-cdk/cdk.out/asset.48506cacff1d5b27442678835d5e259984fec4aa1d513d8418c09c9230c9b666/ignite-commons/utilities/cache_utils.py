import json

import boto3
import requests as request
from utilities import constant as const
from utilities import log_utils as log_utils
from utilities import prop_reader as prop
from utilities import prop_reader as prop_reader
from utilities import s3_utils as s3_utils
from utilities import validator as validator
from utilities.ignite.feed_runtime_context import FeedRuntimeContext as FeedRuntimeContext

def get_api_key(client_key, autorisation_endpoint):
    api_key = None
    headers = {}
    try:
        headers[const.CLIENT_HEADER] = client_key
        urls = (autorisation_endpoint)
        response = request.get(url=urls, headers=headers)
        response_object = json.loads(response.text.encode('utf8'))

        if response_object["code"] != 200:
            raise Exception(response_object["message"])

        for key in response_object["apigeeKeys"]:
            api_keys = [key["apiKey"]]
            api_key = ''.join(api_keys)

        return api_key

    except Exception as e:
        raise e

def call_invalidate_cache(apigee_clear_cache_api_endpoint, api_keys, client_key, request_context):
    try:

        headers = {}
        headers[const.CLIENT_HEADER] = client_key
        headers[const.API_HEADER] = api_keys
        # TODO: username and password is not used in mule on agigee clear cache check if they are relevent
        username = prop.get_prop(const.apigee_section, "apigee.clear.cache.endpoint.username", request_context)
        passkey = prop.get_prop(const.apigee_section, "apigee.clear.cache.endpoint.pass", request_context)

        response = request.post(apigee_clear_cache_api_endpoint, headers=headers)

        status_code = response.status_code

        if status_code != 200:
            raise Exception("Exception while calling Apigee clear cache enpoint, status received: {}".format(str(status_code)))

        log_utils.print_info_logs("successfully called Apigee clean cache call", request_context)
        return status_code
    except Exception as e:
        log_utils.print_error_logs("exception while calling apigee clean cache endpoint. Error: " + str(e), request_context)
        raise e

def get_sns_client():
    return boto3.client(const.SNS)

def get_sqs_client():
    return boto3.client(const.SQS)

def send_sqs_message(sqs_client, sqs_url, message, request_context):
    try:
        response = sqs_client.send_message(
            QueueUrl= sqs_url,
            MessageBody=json.dumps(message))
        return response
    except Exception as e:
        log_utils.print_error_logs(' - Error while sending sqs message', request_context)
        raise e

def publish_sns_notification(sns_topic_arn, message, subject, request_context):
    try:
        sns_client = get_sns_client()
        response = sns_client.publish(
            TargetArn = sns_topic_arn,
            Message = json.dumps({'default': json.dumps(message)}),
            MessageStructure = 'json',
            Subject = subject
        )
        log_utils.print_info_logs(' - sns message published with id : '+response['MessageId'], request_context)
        return response
    except Exception as e:
        log_utils.print_error_logs(' - Error while publish sns message', request_context)
        raise e

def get_cache_clear_full_endpoint(request_context):
    try:
        cache_clean_endpoint = prop.get_prop(const.apigee_section, "apigee.clear.cache.endpoint.url", request_context)
        cache_name = prop.get_prop(const.apigee_section, "apigee.clear.cache.name", request_context)
        return cache_clean_endpoint.format(cache_name)
    except Exception as e:
        raise e
    

    
