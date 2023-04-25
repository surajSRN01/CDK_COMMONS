from typing import List
from commons.field_parser_config import FieldParserConfig, FieldParsingStatus
from commons.sheet_parser_config import SheetJoinType, SheetParserConfig
from models import model_map
from models.circular_dependency_map import CircularDependencyMap
from utilities import log_utils as log_utils
from utilities import transformer as transformer
from utilities import validator as validator
from utilities.bean_utils import BeanUtils
from utilities.parsers.commons import field_parser as field_parser
from utilities.parsers.commons import parser_reflection_utils as parser_reflection_utils
from aggregators.rules.aggregator_utils import rules_utils as rules_utils

"""
Iterate through the given sheet and parse its all field using field parsers.
"""
DOT = "."

def parser_sheet(sheetParserConfig: SheetParserConfig):

    # instantiate empty model class
    target_object_class = model_map.get_model_class_by_config(sheetParserConfig.parser_result.clazz)
    target_model_object = target_object_class()

    # find out the joinType of sheet
    sheet_join_type = sheetParserConfig.sheet_join_type
    # beanUtils = BeanUtils(target_model_object, False, False, True)

    if (sheet_join_type == SheetJoinType.NO_JOIN):
        fieldParserConfig = FieldParserConfig.create_field_parser_config_from_sheet_parser_config(
            target_model_object, sheetParserConfig)
            
        field_parser.init_object(fieldParserConfig)
        if fieldParserConfig.status == FieldParsingStatus.SUCCESS:
            if sheet_join_type == SheetJoinType.HAS_JOIN_BUT_NO_TARGET_LIST_PATH:
                sheet_config = sheetParserConfig.sheet
                joinMap = getJoinMap(sheet_config["join"]["joinBy"], fieldParserConfig.row, sheetParserConfig)
                beanUtils = BeanUtils(target_model_object, joinMap.keys(), False, False, True)
                objects: List = parser_reflection_utils.getObjectsFromList(sheetParserConfig.parser_result.results, joinMap)
                beanUtils.mergeObjects(target_model_object, objects, False)
            else:
                sheetParserConfig.parser_result.report.add_passes()
                sheetParserConfig.parser_result.add_result(target_model_object)

    elif (sheet_join_type == SheetJoinType.HAS_JOIN_BUT_NO_TARGET_LIST_PATH):
        print("processing sheet, has join but no targetListPath")
        print("######## Implemenatation for processing sheet for type 2 is pending")
    elif (sheet_join_type == SheetJoinType.HAS_JOIN_AND_TARGET_LIST_PATH):
        process_row_on_sheet_with_join_and_target_list_path(sheetParserConfig)
    else:
        sheet_config = sheetParserConfig.sheet
        raise Exception("Sheet at index: " + str(sheet_config["index"])+" not has valid join type, "+ 
                        "does not match any condition[(join not exist], or "+
                        "(join exist but no targetListPath inside join) or "+
                        "(join exist and targetJoinPath inside join exist)]")

"""
Desciption of Joining logic:
----------------------------------------
Model: Person(PersonId, Name, Department)
----------------------------------------
Sheet 1:
PersonId Name
P1       N1 
P2       N2

Sheet 2:
PersonId Department:
P1       D1 
P2       D2
------------------------
sheet 1 config:
[{personId = 0, name=1}]
------------------------
sheet 2 config:
[joinBy:[{targetListPath:personId}], {personId = 0, department=1}]
------------------------
This method pull the matching row wrt to targetListPath from provided list of objects
transofrom the matching row to matched objects of provided list.

Add the end:
Model: Person(PersonId, Name, Department) will have, data like:
[(name:P1, name: N1, department:D1), (name:P2, name: N2, department:D2)]
"""
def process_row_on_sheet_with_join_and_target_list_path(sheetParserConfig: SheetParserConfig):
    sheet_config = sheetParserConfig.sheet
    
    # step1. Get matching rows from target sheet which matches joinBy clauses
    joined_rows = get_rows_matches_join(sheetParserConfig)
    if (len(joined_rows) == 0):
        log_utils.print_debug_logs(message="sheet: "+ str(sheet_config["index"]) +
                  " does not have matching rows from master sheet, skippling joinin part", request_context=sheetParserConfig.request_context)
        return

    # step 2. Join Rows
    target_class = get_join_target(sheetParserConfig)    

    success = True
    for joined_row in joined_rows:  
        target_instance = target_class()

        # Step3. Parse, using init
        fieldParserConfig = FieldParserConfig.create_field_parser_config_from_sheet_parser_config(
                target_instance, sheetParserConfig)

        field_parser.init_object(fieldParserConfig)

        success = True
        if (fieldParserConfig.status == FieldParsingStatus.SUCCESS):
            # Step 4. assign|append value on target 
           set_value_on_target(joined_row, target_instance, sheetParserConfig)
        else:
            sheetParserConfig.parser_result.report.add_failed(fieldParserConfig.error_message)
            success = False
            break

    if success == False:
        sheetParserConfig.parser_result.report.add_failed("Unable to set value from joining sheet from index: "+ str(sheet_config["index"]))


def is_sheet_join(sheet):
    join_has_targetListPath = "join" in sheet and "targetListPath" in sheet["join"]
    return join_has_targetListPath and not validator.is_nan(sheet["join"]["targetListPath"])

"""
Create a Map where the keys are join by columns and value are there corresponding value
from the sheet
"""
def get_joint_map(joinsBy: list, row, model_clazz_type):
    nameValueMap = {}
    for joinBy in joinsBy:
        value = None
        if ("value" in joinBy["source"] and not validator.is_nan(joinBy["source"]["value"])):
            value = joinBy["source"]["value"]
        else:
            value = get_field_value(joinBy, row)
        if value == None or validator.is_nan(value):
            continue
        target_type = model_clazz_type[joinBy["target"]].__name__
        value = convert(joinBy, value, target_type=target_type)
        if value == None and validator.is_nan(value):
            continue
        nameValueMap[joinBy["target"]] = value

    if(len(nameValueMap.keys()) != len(joinsBy)):
        return {}

    return nameValueMap

"""
Get all the results from the provided list which matches the joinMap
"""
def get_rows_matches_with_join_columns(results: list, joinMap):
    matching_rows = []
    for result in results:
        objectMatch = True
        for field_name in joinMap.keys():
            if rules_utils.getOrDefault(result, field_name) == None:
                objectMatch = False
                break
            value = get_value_from_model(result, field_name)
            if (value == None or value != joinMap[field_name]):
                objectMatch = False
                break
        if (objectMatch):
            matching_rows.append(result)

    return matching_rows


def get_value_from_model(model, field):
    if (isinstance(model[field], list)):
        print("###### TODO: implement get_value_from_model")
        print("get_value_from_model: drill it down further to get type of list to values")
    if (isinstance(model[field], dict)):
        print("###### TODO: implement get_value_from_model")
        print("get_value_from_model: drill it down further to get type of object to values")

    return model[field]

# if we need different parser types, than implement this is specific parser type
def get_field_value(joinBy, row):
    return row[int(joinBy["source"]["index"])]

def convert(field_config, value, target_type):
    out = None
    if (target_type == 'str'):
        out = transformer.convert_to_string(value, field_config)
    elif (target_type == 'int'):
        out = transformer.convert_to_int(value, field_config)
    elif (target_type == 'float'):
        out = transformer.convert_to_float(value, field_config)
    elif (target_type == 'bool'):
        out = transformer.convert_to_boolean(value)
    elif (target_type == 'date'):
        out = transformer.convert_to_date(value, field_config)
    else:
        print("##### TODO: get_field_value method anticipate a single type value")
    return out


def get_target_class_type(target_object_class, target_list_path: str): 

    if(CircularDependencyMap.is_referenced_class(target_object_class, target_list_path)):
        if(DOT in target_list_path):
            print("##### TargetListPath with circular and nested is not supported yet")
            raise Exception("TargetListPath with circular and nested is not supported yet")
            
        return target_object_class, target_list_path
        
    if(DOT in target_list_path):
        # iterate through nested tagetListPath
        index_of_first_dot = target_list_path.find(DOT)
        target_list_path_parent = target_list_path[0 : index_of_first_dot]
        target_list_path_target = target_list_path[index_of_first_dot+1:]    
        return get_target_class_type(target_object_class.__annotations__[target_list_path_parent], target_list_path_target)
    
    if hasattr(target_object_class.__annotations__[target_list_path], "__args__"):
       return target_object_class.__annotations__[target_list_path], target_list_path
   
    return target_object_class, target_list_path

def get_rows_matches_join(sheetParserConfig: SheetParserConfig):
    sheet_config = sheetParserConfig.sheet
    target_object_class = model_map.get_model_class_by_config(sheetParserConfig.parser_result.clazz)
    
    join_columns_value_map = get_joint_map(
            sheet_config["join"]["joinBy"], sheetParserConfig.row, target_object_class.__annotations__)

    existing_results = sheetParserConfig.parser_result.results
    
    joined_rows = []
    if(len(existing_results) > 0 and len(join_columns_value_map.keys()) > 0):
        joined_rows = get_rows_matches_with_join_columns(existing_results, join_columns_value_map)

    return joined_rows

def get_join_target(sheetParserConfig: SheetParserConfig):

    target_list_path_clazz = None
    target_class = model_map.get_model_class_by_config(sheetParserConfig.parser_result.clazz)
    target_list_path = sheetParserConfig.sheet["join"]["targetListPath"]
    
    # Get target type
    target_class_type, target_list_path = get_target_class_type(target_class, target_list_path)

    # Get target class type
    type_of_target_list_path = None
    try:
       type_of_target_list_path = target_class_type.__annotations__[target_list_path]
    except:
        type_of_target_list_path = target_class_type.__args__[0]

    if(type_of_target_list_path != None):
        return type_of_target_list_path

    if(type_of_target_list_path == None):
        target_list_path_clazz, target_type = CircularDependencyMap.get_referenced_class(target_class_type, target_list_path)
        if(target_list_path_clazz == None):
            raise Exception("Manage circular dependency manullay")
            
    target_list_path_class_instance = None

    # first check if the target is list or object
    if hasattr(target_list_path_clazz, "__args__"):
        # is list
        target_list_path_class_instance = target_list_path_clazz.__args__[0]
    else:
        # is object
        target_list_path_class_instance = target_list_path_clazz

    return target_list_path_class_instance

# target_object == Group instance, value = Dealer instance, joinPath = ignite
def set_value_on_target(target_object, value, sheetParserConfig):
    sheet_target_list_path = sheetParserConfig.sheet["join"]["targetListPath"]
    # 1. targetListPath is a property of target_object
    if(hasattr(target_object, sheet_target_list_path)):
        if(isinstance(target_object[sheet_target_list_path], list)):
            target_object[sheet_target_list_path].append(value)
        else:
            # target_object[sheet_target_list_path] = value
            set_value_on_nested_target(sheetParserConfig.sheet["fields"], value, target_object[sheet_target_list_path])
        return

     # 2. targetListPath is a nested property of target_object
    if(DOT in sheet_target_list_path):
        # recersivly create parent of targetListPath in target_object
        # if targetListPath = contact.websites then first  create contact on target_object
        # and on the new created object assign|append value 
        create_target_object_recursivly_and_set_value(target_object, value, sheet_target_list_path, True) 
        return

    # 3. targetListPath is a property of target_object meta data
    # contact.websites in Group
    if(DOT not in sheet_target_list_path):
        type = get_member_type_from_class(target_object, sheet_target_list_path)
        if(hasattr(target_object, sheet_target_list_path)):
            if(type == "list"):
                target_object[sheet_target_list_path].append(value)
            elif(type == "object"):
                target_object[sheet_target_list_path] = value    
        else:
            if(type == "list"):
                target_object[sheet_target_list_path] = []
                target_object[sheet_target_list_path].append(value)
            elif(type == "object"):
                target_object[sheet_target_list_path] = value 
        return

def create_target_object_recursivly_and_set_value(target_object, value, sheet_target_list_path, has_remaning):
    
    if(has_remaning == False):
        return

    index_of_first_dot = sheet_target_list_path.find(DOT)
    # contact
    target_list_path_parent = sheet_target_list_path[0 : index_of_first_dot]
    # websites
    remaning_path = sheet_target_list_path[index_of_first_dot+1 : ]

    # first create the parent object if not exists
    # Group
    if(not hasattr(target_object, target_list_path_parent)):
        type = get_member_type_from_class(target_object, target_list_path_parent)
        if(type == "object"):
            target_object[target_list_path_parent] = target_object.__class__.__annotations__[target_list_path_parent]()
        elif(type == "list"):
            target_object[target_list_path_parent] = []
        
    if(DOT in remaning_path):
        create_target_object_recursivly_and_set_value(target_object[target_list_path_parent], remaning_path,True)
    else:
        target_object_nested = target_object[target_list_path_parent] 
        type = get_member_type_from_class(target_object_nested, remaning_path)
        if(type == "object"):
            target_object_nested[remaning_path] = value
        elif(type == "list"):
            if(not hasattr(target_object_nested, remaning_path)):
                target_object_nested[remaning_path] = []        
            target_object_nested[remaning_path] = value[remaning_path]

def get_member_type_from_class(object, property):
    member = object.__class__.__annotations__[property]
    if(hasattr(member, "__annotations__")):
        return "object"
    elif(hasattr(member, "__args__")):
        return "list"
    return None    


def getJoinMap(joinBy: List, row, sheetParserConfig: SheetParserConfig):
    nameValueMap = {}
    for fieldConfig in joinBy: 
        value = None
        config = fieldConfig["source"]
        if "source" in fieldConfig and "value" in fieldConfig["source"] and not validator.is_nan(fieldConfig["source"]["value"]):
            value = config["value"]
        else:
            value = sheetParserConfig.parser.get_value(row, config["index"])
        nameValueMap[fieldConfig["target"]] = convert(fieldConfig, value, type(value))
    return nameValueMap

def set_value_on_nested_target(fields: list, parentValue: any, parentTarget: any):
    for field in fields:
        target = field["target"] if "target" in field else None
        if target is None: continue
        if not hasattr(parentValue, target): continue
        parentTarget[target] = parentValue[target]