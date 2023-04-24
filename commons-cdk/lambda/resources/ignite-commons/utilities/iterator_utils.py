import json
from utilities import transformer as transformer  
def iterator_remove(objects_list, object_to_remove):
    objects_list = list(filter(lambda o: hash(o) != hash(object_to_remove), objects_list))

def transform_object(source_object, keys: list):
    transform_object = {}
    for key in keys:
        transform_object[key] = source_object[key]
    return transform_object

def hash(object):
    return hash(json.dumps(object, sort_keys=False))

"""
Remove duplicate from array where an entry has more than one occurence
from provided keys
    Example:
        [{"id": 1, "name": "One", "type": "X"}, {"id": 2, "name": "two", "type": "Y"}, {id: 2, "name": "One", "type": "Z"}]
        remove duplicate object based upon keys, so in the example remove object based 
        on id and name if has duplicate.
Use config to supply additional parameters to configure this method.        
"""
config = {
 "ignore_case": False,
 "json-dump": True
}
def remove_duplicate(list:list, keys:list, config=config):
    try:
        hashes = []
        for entry in list:
            keys_value = {}
            for key in keys:
                keys_value[key] = entry[key]
            if(config["ignore_case"]) == True:
                keys_value[key] = str(entry[key]).upper()
            if config["json-dump"] == True:
                hash_value = str(keys_value)
            else:
                hash_value = keys_value

            if hash_value in hashes:    
                list.remove(entry)
            else:
                hashes.append(hash_value)

    except Exception as e:
        raise e

def is_empty(collection):
    if(collection == None):
        return True
    if(not isinstance(collection, list)):
        return True
    return len(collection) == 0   

def group_by(elements, keys):
    group = {}
    for item in elements:
        key = str(list(map(lambda key: item[key], keys)))
        if key not in group.keys():
            group[key] = []
        group[key].append(item)
    
    return group 

# get collection from object else default empty array
def getOrDefault(object, collectionName):
    if not transformer.is_object_has_valid_property(object, collectionName):
        return []
    if is_empty(object[collectionName]):
        return []
    return object[collectionName]