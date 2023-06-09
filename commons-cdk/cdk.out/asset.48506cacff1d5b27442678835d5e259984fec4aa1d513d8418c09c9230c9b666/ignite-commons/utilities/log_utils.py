import logging
from utilities.ignite.feed_runtime_context import (FeedRuntimeContext, Status)


list_of_log = []
feed_runtime_context = FeedRuntimeContext.get_instance()
    
# create logger
def get_logger() :
    logger = logging.getLogger()
    if not logger.handlers:
        c_handler = get_stream_handler()
        logger.addHandler(c_handler)
    logger.setLevel(logging.DEBUG)
    return logger

#create stream handler
def get_stream_handler() :
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.INFO)
    # Create formatter and add it to handler
    c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    return c_handler

#Create file handler

def get_file_handler(file_path) :
    f_handler = logging.FileHandler(file_path)
    f_handler.setLevel(logging.DEBUG)
    # Create formatters and add it to handlers
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)

def print_info_logs(message):
    
    feed_runtime_context.logger.info('x_correlation_id="'+feed_runtime_context.x_correlation_id+'", environment="' +
                feed_runtime_context.env + '", job="' + feed_runtime_context.job_name + '" ' + message)

def print_error_logs(message):
    feed_runtime_context.logger.error('x_correlation_id="'+feed_runtime_context.x_correlation_id+'", environment="' +
                 feed_runtime_context.env + '", job="' + feed_runtime_context.job_name + '" ' + str(message))

def print_debug_logs(message):
    feed_runtime_context.logger.debug('x_correlation_id="'+feed_runtime_context.x_correlation_id+'", environment="' +
                 feed_runtime_context.env + '", job="' + feed_runtime_context.job_name + '" ' + str(message))

def add_info_logs(message):
    list_of_log.append(str(message))

def get_info_logs():
    return list_of_log