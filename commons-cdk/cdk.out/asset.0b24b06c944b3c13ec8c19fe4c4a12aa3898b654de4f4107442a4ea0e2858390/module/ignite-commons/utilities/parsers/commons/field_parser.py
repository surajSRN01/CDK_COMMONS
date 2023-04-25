import copy
import traceback
from exception.error_code import ErrorCode
from exception.etl_exception import EtlException
from commons.field_parser_config import FieldParserConfig, FieldParsingStatus
from commons.list_type import ListType
from models.model_map import MODEL_MAP
from utilities import log_utils as log_utils
from utilities import transformer as transformer
from utilities import iterator_utils as iterator_utils
from utilities import validator as validator

import json
from types import SimpleNamespace

VALIDATION_ERROR = "Sheet: '{sheet_index}'. Item: '{row}' >> validation error - {error}"
PATH_SEPARATOR = "."
TARGET_LABLE_VALUE = "labelValue"

"""
Iterate through each field config, get the value of corresponsing cell from the
sheet and apply that value to model provided in FieldParserConfig
"""
def init_object(fieldParserConfig: FieldParserConfig):
    try:
        
        if fieldParserConfig.is_completed():
            # iteration of all field_config at any level is completed at this point
            fieldParserConfig.mark_success()   
            return      

        is_all_field_config_covered = len(fieldParserConfig.fields) == fieldParserConfig.current_field_config_index     
        if is_all_field_config_covered and fieldParserConfig.parent_field_parser_config is not None:
            # if iteration for any level nested fieldConfig is completed, 
            # call the init_object method again with parent of current fieldParserConfig to 
            # completed its  fields:
            # flow travel from child to parrent's next sibling 
            fieldParserConfig.parent_field_parser_config.current_field_config_index +=1
            return init_object(fieldParserConfig.parent_field_parser_config)

        current_field_config = fieldParserConfig.fields[fieldParserConfig.current_field_config_index]    
        
        field_target = None
        try:
            field_target = current_field_config["target"]
        except Exception as e:
            raise EtlException(str(e), ErrorCode.IGNORE_ABLE)
        
        hasType = "type" in current_field_config
        dataType = getOrDefaultFromJson(current_field_config, "data_type")
        model = fieldParserConfig.model
         
        if(hasType and current_field_config["type"] == "object"):
            is_child = False            
            if not(hasattr(model, field_target)) and not(isinstance(model, list)):
                model[field_target] = model.__class__.__annotations__[field_target]()
                model = fieldParserConfig.model[field_target]
            else:
                is_child = True

            fields_config = current_field_config["fields"]
            nested_field_config = fieldParserConfig.clone(model, fields_config)

            if is_child and fieldParserConfig.current_list_type is not None:
                nested_field_config.child_class_type = fieldParserConfig.current_list_type.class_type.__annotations__[field_target]
                                
            return init_object(nested_field_config)
        elif(hasType and current_field_config["type"] == "list"):
            # if "list_type" in current_field_config:
            #     return create_value_list_recursivly(fieldParserConfig)
                
            current_list_type = None
            if not(hasattr(model, field_target)):
                # initailse target list
                fieldParserConfig.model[field_target] = []
                target_field = fieldParserConfig.model.__annotations__[field_target]
                current_list_type = target_field.__args__[0].__name__
            
            is_primitive_list = current_list_type in ('str', 'int', 'float', 'bool')    

            if(is_primitive_list):
               listType = ListType(True, current_list_type)
            else:
                list_type_object = fieldParserConfig.model.__annotations__[field_target].__args__[0]
                listType = ListType(False, list_type_object)
                model = fieldParserConfig.model[field_target]
            
            fields_config = []
            if "fields" not in current_field_config:
                proxy_fields = copy.copy(current_field_config)
                # remove type attribute to avoid same iteration next time
                del proxy_fields["type"]
                fields_config.append(proxy_fields)
            else:
                fields_config = current_field_config["fields"]

            nested_field_config = fieldParserConfig.clone(model, fields_config)
            nested_field_config.current_list_type = listType

            if dataType is not None and dataType == "jsonarray":
                # create list of object from provided fieldConfig array, attach it to target
                values_list = create_values_list_from_field_config(nested_field_config)
                   
                # move the parsing to next target
                fieldParserConfig = nested_field_config.parent_field_parser_config 
                if not iterator_utils.is_empty(values_list):
                    fieldParserConfig.model[field_target] = values_list

                fieldParserConfig.current_field_config_index += 1
                if fieldParserConfig.parent_field_parser_config != None:
                    current_index = fieldParserConfig.current_field_config_index
                    last_index = len(fieldParserConfig.fields)
                    if(current_index == last_index):
                        fieldParserConfig = fieldParserConfig.parent_field_parser_config
                        fieldParserConfig.current_field_config_index += 1 
                
                return init_object(fieldParserConfig)
            
            return init_object(nested_field_config)
        else:
            # single value field
            set_value_on_field(fieldParserConfig)

            if fieldParserConfig.status == FieldParsingStatus.FAILED:
                return

            fieldParserConfig.current_field_config_index += 1
            if fieldParserConfig.parent_field_parser_config != None:
                current_index = fieldParserConfig.current_field_config_index
                last_index = len(fieldParserConfig.fields)
                if(current_index == last_index):
                    fieldParserConfig = fieldParserConfig.parent_field_parser_config
                    fieldParserConfig.current_field_config_index += 1     
                
            return init_object(fieldParserConfig)
         
    except EtlException as e:
        if e.error_code is not ErrorCode.IGNORE_ABLE:
            # Fail a row for if there indeed an issue ralated to misning
            # required thing or simmiller to it
            fieldParserConfig.mark_failed(e.message)
        return
                 
    except Exception as e :
        # log_utils.print_error_logs("Exception while parsing sheet at index: "+ str(fieldParserConfig.sheet_index), fieldParserConfig.request_context)  
        validation_error = VALIDATION_ERROR.format(sheet_index = fieldParserConfig.sheet_index, row = fieldParserConfig.row_index, error = "Unable to read/find value")
        fieldParserConfig.mark_failed(validation_error)
        # traceback.print_exc()
        return     

def apply_value_on_field(value, fieldConfig, fieldParserConfig: FieldParserConfig):
    try:
        model = fieldParserConfig.model
        target = fieldConfig["target"]
        is_valid, error = validator.field_validation(value, fieldConfig)
       
        if(is_valid == False):
            return False, error

        if(validator.is_nan(value)):
            # ok to not have value for non-required type
            return True, ""

        is_json_type = "data_type" in fieldConfig and fieldConfig["data_type"] == "json"
        if is_json_type:
            pass
        else:
            value = convert_value_to_field_type(value, fieldConfig, model, fieldParserConfig.current_list_type)

        if(validator.is_nan(value)):
            # ok to not have value for non-required type
            return True, ""

        if isinstance(value, list) or is_json_type:
            set_values_on_model(value, target, fieldParserConfig, is_json_type)
        else:     
            # single value fields
            model[target] = value
        return True, ""   
    except Exception as e:
        log_utils.print_error_logs("exception while apply value on field: " +fieldConfig["target"], fieldParserConfig.request_context)
        traceback.print_exc()
        return False, str(e)         

def convert_value_to_field_type(value, field_config, model, listType:ListType):
    if not field_config["target"] == TARGET_LABLE_VALUE and listType is None:
        out = cast_value_to_target_type(value, field_config, model)
    elif listType is not None:
        value = transformer.convert_value_to_single_valued_list(str(value), field_config)
        out = cast_values_to_target_type(value, field_config, listType)
    else:
        out = str(value)
    return out

def set_value_on_field(fieldParserConfig: FieldParserConfig):
    current_field_config = fieldParserConfig.fields[fieldParserConfig.current_field_config_index]
    
    has_source = True
    if("source" not in current_field_config):
        has_source = False

    if(has_source == False):        
        validations = get_validations(current_field_config)
        if(len(validations) > 0):
            validation_types = ", ".join(validations)
            error_message = "source not exist on required:("+validation_types+") field config for target: '"+current_field_config["target"]+"'"
            
            error = "sheet: " + str(fieldParserConfig.sheet_index) +", Item: "+ str(fieldParserConfig.row_index) +", error: "+ error_message
            raise EtlException(error, ErrorCode.MISSING_REQUIRED_COLUMN)
        else:
            # ok to not have source for a non required fields.
            return

    field_source = current_field_config["source"]
            
    if ("value" in field_source and field_source["value"] != ""):
        value = field_source["value"]
    else:
        sourceIndex = fieldParserConfig.parser.get_source_field(field_source)
        rowLength = len(fieldParserConfig.row)
        if(isinstance(sourceIndex, int) and sourceIndex >= rowLength):
            value = None
        else:
            if "data_type" in current_field_config and current_field_config["data_type"] == "json":
                value = fieldParserConfig.row[field_source["index"]]
            if fieldParserConfig.parser.get_parser_type() == "fixed":
                value = fieldParserConfig.parser.get_value(fieldParserConfig.row, field_source)
            else:
                value = fieldParserConfig.parser.get_value(fieldParserConfig.row, field_source["index"])
                
                if(not validator.is_nan(value) and isinstance(value, list)):
                    # if the value for a field return from parser is list then join then first
                    # based on seprator config
                    value = transformer.join_list_by_field_config(value, current_field_config)

        has_default_value = "defaultValue" in field_source
        if (validator.is_nan(value) and (has_default_value and field_source["defaultValue"] != "")):
            value = field_source["defaultValue"]

    is_init_sucess, error_message = apply_value_on_field(value, current_field_config, fieldParserConfig)
        
    if(is_init_sucess == False):
        error = "sheet: " + str(fieldParserConfig.sheet_index) +", Item: "+ str(fieldParserConfig.row_index) +", error: "+ error_message
        raise EtlException(error, ErrorCode.MISSING_REQUIRED_COLUMN)  

def get_validations(fieldConfig):
    if("validations" not in fieldConfig):
        return []

    validations = []
    for key in fieldConfig["validations"]:
        if(fieldConfig["validations"][key] == "true"):
            validations.append(key)

    return validations

def cast_value_to_target_type(value, field_config, model):
    try:
        target = field_config["target"]
        if(model in ('str', 'int', 'float', 'bool')):
            type = model
        else:
            target_field_type = model.__class__.__annotations__[target] 
            if hasattr(target_field_type, "__name__"):
                type = target_field_type.__name__
            elif hasattr(target_field_type, "__args__"):
                type = get_list_type(target_field_type.__args__)   
        if(type == "str"):
            return transformer.convert_to_string(value, field_config) 
        elif(type == "float"):
            return transformer.convert_to_float(value, field_config)  
        elif(type == "bool"):
            return transformer.convert_to_boolean(value, field_config)
        elif(type == "int"):
            return transformer.convert_to_int(value, field_config)
        elif(type == "date"):
            return transformer.convert_to_date(value, field_config)
        else:
            raise e("Invalid type: "+ type)        
    except Exception as e:
        # print("Invalid value: '"+str(value)+"' detected for non required field: '"+target+"' in: '"+model.__class__.__name__+"'")
        return ""

def cast_values_to_target_type(values, field_config, list_type: ListType):
    casted_values = []
    for value in values:
        if(not validator.is_nan(value)):
            if list_type.is_premitive: 
                value = cast_value_to_target_type(value, field_config, list_type.class_type)
            casted_values.append(value)
    return casted_values

"""
ISSUE:
Dealer has a list of certifications, certification has image one to one
Dealer > list(certification > tuple(image)
    certifications after resolving, containing list of image instead of having list of certificate
    and then each certificate should have image inside.
Call this method recursivly for refactor
"""
def set_values_on_model(values, target, fieldParserConfig: FieldParserConfig, is_json_type: bool):
    list_type = fieldParserConfig.current_list_type
    model = fieldParserConfig.model
    child_class_type = fieldParserConfig.child_class_type
  
    has_child = False
    if(child_class_type != None and list_type != None):
        has_child = True
        initailse_parent_list(model, list_type, child_class_type, len(values))
    
    is_set_in_child = False
    if isinstance(model, list) and (isinstance(values, list) or is_json_type):
        if(has_child and len(values) > 0):
            current_target_index = fieldParserConfig.parent_field_parser_config.current_field_config_index
            current_target = fieldParserConfig.parent_field_parser_config.fields[current_target_index]["target"]
            
            targets = []
            for index in range(len(model)): 
                model[index][current_target] = child_class_type()
                targets.append(model[index][current_target])
            if is_json_type:
                targets[0][target] = str()
                targets[0][target] = values
                return
                 
            previous_model = fieldParserConfig.model
            previous_list_type = fieldParserConfig.current_list_type
            for index, value in enumerate(values):
                fieldParserConfig.model = targets[index]
                fieldParserConfig.current_list_type = None
                current_field_config = fieldParserConfig.fields[fieldParserConfig.current_field_config_index]
                apply_value_on_field(value, current_field_config, fieldParserConfig)
            fieldParserConfig.model = previous_model
            fieldParserConfig.current_list_type = previous_list_type
            is_set_in_child = True
                
        if(len(model) == 0):
            if(list_type.is_premitive):
                model = values
            else:    
                for v in values:
                    list_type_object = fieldParserConfig.current_list_type.class_type()
                    list_type_object[target] = v
                    model.append(list_type_object)
        elif is_set_in_child == False:
            for index, m in enumerate(model):
                if index < len(values):
                    m[target] = values[index]

    elif fieldParserConfig.child_class_type != None:
        for v in values:
            list_type_object = fieldParserConfig.current_list_type.class_type()
            list_type_object[target] = v
            model.append(list_type_object)
    else:
        # primitive|str list
        model[target] = values        
        

def create_current_target_instance(fieldParserConfig: FieldParserConfig):
    if(fieldParserConfig.child_class_type == None):
        return fieldParserConfig.current_list_type.class_type()
    else:
        return fieldParserConfig.child_class_type()

def initailse_parent_list(model, container_type: ListType, child_class_type, total_object_to_create):
    if(total_object_to_create == 0):
        return
    
    for index in range(total_object_to_create):
        model.append(container_type.class_type())
        
# get the type for premetive list
# str|float|int        
def get_list_type(list_class):
    list_class_name = str(list_class)
    if "str" in str(list_class_name):
        return "str"
    if "float" in str(list_class_name):
        return "float"
    if "int" in str(list_class_name):
        return "int"

def getOrDefaultFromJson(object, property):
    if transformer.is_json_has_valid_property(object, property):
        return object[property]
    return None

def create_values_list_from_field_config(fieldParserConfig: FieldParserConfig) -> list:
    # field config will be array at this point:
    values_list = fieldParserConfig.model
    
    for index, fields in enumerate(fieldParserConfig.fields):
        target_object = fieldParserConfig.current_list_type.class_type()
        nested_field_config = fieldParserConfig.clone(target_object, fields)
        nested_field_config.current_list_type = None
        values_list.append(target_object)
        for nextIndex in range(len(fields)):
            if "type" in fields[nextIndex] and fields[nextIndex]["type"] == "object":
                populateValuesForObject(target_object, fields[nextIndex]["target"], nested_field_config, fields[nextIndex]["fields"])
            else:
                nested_field_config.current_field_config_index = nextIndex
                set_value_on_field(nested_field_config)
   
    return values_list

def populateValuesForObject(model, field_target, fieldParserConfig:FieldParserConfig, fields_config):
    
    if not(hasattr(model, field_target)) and not(isinstance(model, list)):
        model[field_target] = model.__class__.__annotations__[field_target]()

    nested_field_config = fieldParserConfig.clone(model[field_target], fields_config)                     
    nested_field_config.callRecursive = False
    nested_field_config.current_field_config_index = 0
    init_object(nested_field_config)

# def create_value_list_recursivly(fieldParserConfig: FieldParserConfig):
#     model = fieldParserConfig.model
#     current_field_config = fieldParserConfig.fields[fieldParserConfig.current_field_config_index]
#     target_type = current_field_config["target"] 
#     class_type = get_class_by_name(current_field_config["list_type"])
#     object = class_type
#     model[target_type] = [object]
#     inner_list = get_class_by_name(current_field_config["fields"][0]["list_type"])
#     model[target_type][0] = inner_list()
#     value_container = model[target_type][0]
#     nested_field_config = fieldParserConfig.clone(value_container, current_field_config["fields"][0]["fields"])
#     nested_field_config.current_list_type = None
#     nested_field_config.callRecursive = False
#     for nextIndex in range(len(nested_field_config.fields)):
#         nested_field_config.current_field_config_index = nextIndex
#         set_value_on_field(nested_field_config)