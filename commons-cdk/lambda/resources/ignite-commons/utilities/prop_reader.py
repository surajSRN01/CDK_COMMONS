import os
import configparser
import utilities.constant as const
import utilities.log_utils as log_utils
import zipfile
import io
import json
from utilities.ignite.feed_runtime_context import (FeedRuntimeContext, Status)


def get_prop(prop_section, prop_name) :
    feed_runtime_context = FeedRuntimeContext.get_instance()
    
    log_utils.print_info_logs(' - Started reading properties , properties name :'+prop_name+' and properties section :'+prop_section)
    config = configparser.ConfigParser()
    
    try :
        dir_path=os.path.dirname(os.path.realpath(__file__))
        parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
        my_file = os.path.join(parent_dir_path+'/'+const.PROPERTIES_PATH+feed_runtime_context.env+'.properties')
        if(parent_dir_path.__contains__('.zip')) :
            zf = zipfile.ZipFile(parent_dir_path)
            zf_config = zf.open(const.PROPERTIES_PATH+feed_runtime_context.env+'.properties', "r")
            zf_config = io.TextIOWrapper(zf_config)
            config.read_file(zf_config)
        else :
            with open(my_file,'r') as configfile:       
                config.read_file(configfile)
    
        prop = config.get(prop_section, prop_name)
        return prop
    except configparser.NoSectionError as e :
        log_utils.print_error_logs(' - Error while reading properties')
        raise e
    except Exception as e :
        log_utils.print_error_logs(' - Error while reading properties')
        raise e

def get_config_prop(prop_section, prop_name, absolute_file_path):
        
    log_utils.print_info_logs(' - Started reading json properties, properties name : '+prop_name+' and properties section : '+prop_section)
    try:
        data = None 
        dir_path = os.path.dirname(os.path.realpath(__file__))
        parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
        my_file = os.path.join(parent_dir_path+'/' + absolute_file_path)
        if(parent_dir_path.__contains__('.zip')) :
            zf = zipfile.ZipFile(parent_dir_path)
            zf_config = zf.open(absolute_file_path, "r")
            zf_config = io.TextIOWrapper(zf_config)
            data = json.load(zf_config)
        else :
            with open(my_file, 'r') as configfile:
                data = json.load(configfile)
        return data
    except Exception as e:
        log_utils.print_error_logs(' - Error while reading properties' )
        raise e


def get_config_file_content(prop_section, absolute_file_path):
    file_name = absolute_file_path.split("/")[-1]
    
    log_utils.print_info_logs(" - Started reading json properties file: {file_name} and from {from_dir}".format(file_name=file_name, from_dir=prop_section))
    try:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
        if(parent_dir_path.__contains__('.zip')) :
            zf = zipfile.ZipFile(parent_dir_path)
            zf_config = zf.open(absolute_file_path, "r")
            zf_config = io.TextIOWrapper(zf_config)
            data = json.load(zf_config)
        else:
            file = os.path.join(parent_dir_path+'/' + absolute_file_path)
            with open(file, 'r') as configfile: data=json.load(configfile)
        return data
    except Exception as e:
        log_utils.print_error_logs(" - Error while reading properties with absolute path {}".format(absolute_file_path) )
        raise e  
