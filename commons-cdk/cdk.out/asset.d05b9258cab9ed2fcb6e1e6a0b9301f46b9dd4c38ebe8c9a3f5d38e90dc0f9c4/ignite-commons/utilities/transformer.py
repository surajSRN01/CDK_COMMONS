
from datetime import date, datetime
from fileinput import filename

import pyspark.sql.functions as F
import utilities.validator as validator

REGEX_END_OF_LINE = "$"
COMMA = ", "

# function to rename feed file columns to mongoDB columns
def rename_columns(col, col_new, sdf, logger, x_correlation_id) :
    logger.info('x_correlation_id :'+x_correlation_id+ ' -renaming excel columns to mongo DB columns')
    try : 
        sdf = sdf.withColumnRenamed(col, col_new)
        return sdf
    except Exception as e :
        logger.error('x_correlation_id :'+x_correlation_id+ ' - Error while renaming columns')
        logger.error(e)
        raise 

# function to check for nan conversion
def nan_convert(df_g, logger, x_correlation_id) :
    try :
        columns = df_g.columns
        for column in columns:
            if((column == 'mandatory') | (column == 'pii') | (column == 'sensitive') | (column == 'hash')) :
                if((column == 'mandatory') | (column == 'sensitive') | (column == 'hash')) :
                    df_g = df_g.withColumn(column,F.when((F.upper(F.col(column))=='YES'),True).otherwise(False))
                if(column == 'pii') :
                    df_g = df_g.withColumn(column,F.when((F.upper(F.col(column))=='NO'),False).otherwise(True))
            else :
                df_g = df_g.withColumn(column,F.when(F.isnan(F.col(column)),"").otherwise(F.col(column)))
        return df_g
    except Exception as e :
        logger.error('x_correlation_id :'+x_correlation_id+ ' - Error while Nan conversion. FileName :' + filename)
        logger.error(e)
        raise

# function to get targetSystemRoutingId and language
def get_targetSystemRoutingId_language_file_name(file_path, logger, x_correlation_id) :
    try :
        logger.info('x_correlation_id :'+x_correlation_id+ ' - get target system routing id and language from file name')
        file_paths = file_path.split(sep="/")
        file_name_with_ext = file_paths[-1]
        file_name = file_name_with_ext.split('.', 1)[0]
        names = file_name.split("_")
        langauge = names[2]
        targetSystemRoutingId = names[3]
        logger.info('x_correlation_id :'+x_correlation_id+ ' - file name :'+file_name_with_ext)
        return targetSystemRoutingId, langauge, file_name_with_ext
    except Exception as e :
        logger.error('x_correlation_id :'+x_correlation_id+ ' - Error while extracting targetSystemRoutingId and language values from file name')
        logger.error(e)
        raise

def convertToString(value: str, field_config_processing):
    if(value is None):
        return value

    if(field_config_processing is None):
        return value
    
    if("removePrefix" in field_config_processing):
        value = value.removeprefix(field_config_processing["removePrefix"])
    
    if("removeSuffix" in field_config_processing):
        value = value.removesuffix(field_config_processing["removeSuffix"])
    
    if("removeEmpty" in field_config_processing):
        if(field_config_processing["removeEmpty"] == "true" and value == ""):
            value = None

    return value
        
def splitAndTrimItemsObject(value: str, field_config):
    result = []
    if(value == None):
        return result
    else:
        if("separators" not in field_config):
            result.append(value)

        separators = field_config["separators"]["list"]
        for item in value.split(separators):
            if not validator.is_nan(item):
                result.append({field_config["target"]: item})
    return result

def splitAndTrimItems(value: str, field_config):
    result = []
    if(value == None):
        return result
    else:
        if("separators" not in field_config):
            result.append(value)

        if("separators" in field_config):
            separators = field_config["separators"]["list"]
            for item in value.split(separators):
                if not validator.is_nan(item):
                    result.append(item)
    return result

def convertFromString(value: str, field_config):
    result = ""
    if(value == None):
        return result
    else:
        result = []
        if("separators" not in field_config):
            result.append(value)

        separators = field_config["separators"]["list"]
        for item in value.split(separators):
            result.append(item)
    return result

"""
Apply processing configuration rules on <b>valueObj</b>. Additionally, <b>valueObj</b> is trimmed.
"""
def convert_to_string(valueObj, field_config):
    if validator.is_nan(valueObj):
        return ""

    value = str(valueObj)
    is_list = False

    if "separators" in field_config:
        separators_config = field_config["separators"]
        if("list" in separators_config):
            value = str(value).split(separators_config["list"])
            is_list = True
            if("onlyItem" in separators_config):
                is_list = False
                value = value[separators_config["onlyItem"]]
            if is_list:
                is_list = False
                value = ",".join(value)

    if("processing" in field_config):
        processing = field_config["processing"]
        # TODO: Check if these works on each token if the string is comma seprated
        # or consider the comma separted string just as single value
        if("removePrefix" in processing and not validator.is_nan(processing["removePrefix"])):
            value = value.removeprefix(processing["removePrefix"])
        if("removeSuffix" in processing and not validator.is_nan(processing["removeSuffix"])):
            value = value.removesuffix(processing["removeSuffix"]+REGEX_END_OF_LINE)    
        if("removeEmpty" in processing and (bool(processing["removeEmpty"]) and validator.is_nan(value))):
            value = None

    if value == None: return None
    
    if(is_list):
        return value.split(",")

    return value

def convert_to_double(value_obj, field_config):
    value = convertToString(value_obj, field_config)
    if(validator.is_nan(value_obj)):
        return None
    
    if(not "separators" in field_config):
        return value
    
    separator_config = field_config["separators"]
    if(not hasattr(separator_config, "decimal") and not hasattr(separator_config, "group")):
        return value

    print("### TODO: Implement the logic to parse double later")
    """
    decimalSeparator = str(separator_config["decimal"])[0]
    groupingSeparator = str(separator_config["group"])[0]   
    decimalFormatSymbols = DecimalFormatSymbols()
    decimalFormatSymbols.setDecimalSeparator(decimalSeparator)
    decimalFormatSymbols.setGroupingSeparator(groupingSeparator)

    decimalFormat = DecimalFormat()
    decimalFormat.setDecimalFormatSymbols(decimalFormatSymbols)

    parsePosition = ParsePosition(0)
    parse: Number = decimalFormat.parse(value, parsePosition)

    if (parse == None or parsePosition.getIndex() != value.length()):
        return value
    """
    return value

def convert_to_int(valueObj, field_config):
    value = convert_to_string(valueObj, field_config)
    if(validator.is_nan(value)):
        return None

    return int(float(convert_to_double(value, field_config))) 

def convert_to_boolean(valueObj, field_config):
    value = convert_to_string(valueObj, field_config)
    if(validator.is_nan(value)):
        return None    
    
    return get_boolean(value)

def convert_to_date(valueObj, field_config):
    value = convert_to_string(valueObj, field_config)
    
    if(validator.is_nan(value)):
        return None

    separators_config = field_config["separators"] if "separators" in field_config else None
    if(separators_config == None):
        return date(value)
    
    if(not "pattern" in separators_config or validator.is_nan(separators_config["pattern"])):
        return value

    return datetime.strptime(value, separators_config["pattern"])

def get_boolean(value: str):
    if(validator.is_nan(value)):
        return None
    if(isinstance(value, bool)):
        return value
    case_fold_value = value.casefold() 
    false_identifier = ["0".casefold(), "false".casefold(), "n".casefold() , "no".casefold()]
    if case_fold_value in false_identifier: 
        return False

    true_identifier = ["1".casefold(), "true".casefold(),  "y".casefold(), "yes".casefold(),"X".casefold()]                                
    if case_fold_value in true_identifier: 
        return True

    return None

def convert_to_float(valueObj, field_config):
    value = convert_to_string(valueObj, field_config)
    if(validator.is_nan(value)):
        return None
    try:
        return float(value)
    except:
        print("Invalid value detected for type float value: '"+value+"'")

def convert_value_to_list_object(value, field_config):
    values = []
    
    if("processing" in field_config):
        value = convertToString(value, field_config["processing"])

    if(value is None or value == ""):
        return value

    items = splitAndTrimItemsObject(value, field_config)
    if(isinstance(value, list)):
        outList = list(value)
    else:
        outList = []

    for item in items:
        newValue = convertFromString(field_config, item)
        if(newValue == "" or newValue is not None):
            if("validation" in field_config and "isAllowDuplicates" in field_config["validation"] and field_config["validation"]["isAllowDuplicates"]):
                if(newValue not in outList):
                    outList.append(newValue)
            else:
                outList.append(newValue)        

    if(len(outList) > 1):
             out = outList

    return out   

def convert_value_to_single_valued_list(value, field_config):
    
    if("processing" in field_config):
        value = convertToString(value, field_config["processing"])

    if(value is None or value == ""):
        return []

    outList = splitAndTrimItems(value, field_config)
    if("validation" in field_config and "isAllowDuplicates" in field_config["validation"] and field_config["validation"]["isAllowDuplicates"] == False):
        # remove duplicates if allow duplicate is false  
        return [*set(outList)]                  

    return outList

def join_list_by_field_config(values, field_config):

    if(values == None):
        return None

    if(not isinstance(values, list)):
        return values    

    values = filter(lambda v: v != None, values)
    if not "separators" in field_config:
        # default seprator ","
        return COMMA.join(values)
    
    if not "list" in field_config["separators"]:
        return COMMA.join(values)

    seprator = field_config["separators"]["list"]
    return seprator.join(values)

def format_date(date: date, format: str):
    return date.strftime(format)    

"""
Copy value from source object to target object.
source = {"object": source_object, "key": source_key}
target = {"object": target_object, "key": target_key}
"""
def copy_value(source, target, data_type, format):
    source_object = source["object"]
    source_key = source["key"]
    
    target_object = target["object"]
    target_key = target["key"]

    if(not hasattr(source_object, source_key) or validator.is_nan(source_object[source_key])):
        return
    
    if data_type == "date":
        target_object[target_key] = format_date(source_object[source_key], format)
    if data_type == "str":
        target_object[target_key] = source_object[source_key]
    if data_type == "bool":
        target_object[target_key] = bool(source_object[source_key])  
         
def is_json_has_valid_property(json_object, property):
    return property in json_object and not validator.is_nan(json_object[property])        
       
def is_object_has_valid_property(object, property):
    return hasattr(object, property) and not validator.is_nan(object[property])

def try_or_fail(value, datatype):
    try:
        if datatype == "int":
            return int(value)
        if datatype == "float":
            return float(value)
        if datatype == "bool":
            return bool(value)
        else:
            return str(value)
    except:
        return None