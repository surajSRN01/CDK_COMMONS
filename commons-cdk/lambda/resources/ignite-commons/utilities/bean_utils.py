from aggregators.rules.aggregator_utils import rules_utils as rules_utils
import utilities.iterator_utils as iterator_utils
import utilities.validator as validator


class BeanUtils:

    def getPrimitiveTypes():
        primitives = set()
        primitives.add(str.__class__)
        primitives.add(bool.__class__)
        primitives.add(int.__class__)
        primitives.add(float.__class__)
        return primitives

    PRIMITIVE_LIST: set = getPrimitiveTypes()

    entity: None
    replaceNotNull: bool
    replaceWithNull = bool
    replaceRecursive: bool
    mergeByFields: set

    def __init__(self, entity, mergeByFields, replaceNotNull, replaceWithNull, replaceRecursive):
        self.entity = entity
        self.replaceNotNull = replaceNotNull
        self.replaceWithNull = replaceWithNull
        self.replaceRecursive = replaceRecursive
        self.mergeByFields = mergeByFields

    def merge_objects(self, targetObject, matching_objects: list):
        return self.mergeObjects(targetObject, matching_objects, False)

    def mergeObjects(self, join_target, join_sources: list, onlyFirstMatch):
        """
        Merge objects of same type from multiple lists Priority is defined based on optional parameters order: first has
        the lowest priority, last has the highest priority
        """
        if (len(join_sources) == 0):
            return []
        try:
            for join_source in join_sources:
                match = True
                for fieldName in self.mergeByFields:
                    mergingField = self.entity[fieldName]
                    mergeObjectFieldValue = self.getFieldValue(mergingField, join_target)
                    objectIdFieldValue = self.getFieldValue(mergingField, join_source)
                    
                    if mergeObjectFieldValue != None:
                    
                        if (not mergeObjectFieldValue == objectIdFieldValue):
                            match = False
                            break
                    
                    elif objectIdFieldValue != None:
                        match = False
                        break

                if match:
                    self.replaceValuesRecursive(self.entity, join_target, join_source)
                    if onlyFirstMatch:
                        break
                        
        except Exception as e:
            print("")
        
    def merge_objects_list(self, lists):
        aggregatedDataList = []
        for object_list in lists:
            if validator.is_nan(self.mergeByFields) or iterator_utils.is_empty(self.mergeByFields):
                if validator.is_nan(object_list) or iterator_utils.is_empty(object_list):
                    aggregatedDataList.extend(aggregatedDataList)
                    continue

            if validator.is_nan(aggregatedDataList) or iterator_utils.is_empty(aggregatedDataList):
                aggregatedDataList = object_list            
                continue

            if validator.is_nan(object_list) or iterator_utils.is_empty(object_list):
                continue

            for obj in object_list:
                foundMatch = False
                for aggregatedObject in aggregatedDataList: 
                    if self.is_object_equals(obj, aggregatedObject):
                        self.replaceValuesRecursive(aggregatedObject, obj)
                        foundMatch = True
                        break
                    
                if foundMatch == False:
                    aggregatedDataList.append(obj)

        return aggregatedDataList    
    
    
    def getFieldValue(self, merging_field, join_target):
        return rules_utils.getOrDefault(join_target, merging_field)

    def replaceValuesRecursive(self, destination, source):
    
        fields = self.entity.__annotations__
        for field in fields:

            if not hasattr(source, field):
                continue

            destinationFieldValue = self.getFieldValue(field, destination)
            sourceFieldValue = self.getFieldValue(field, source)
            field_type = self.get_field_type(field)
            if field_type == "list":
                temp = []
                if not validator.is_nan(destinationFieldValue):
                    temp.extend(destinationFieldValue)
                if not validator.is_nan(sourceFieldValue):
                    temp.extend(sourceFieldValue)
                destination[field] = temp
            elif field_type == "primitive":
                if validator.is_nan(destinationFieldValue) or self.replaceNotNull:
                    if not validator.is_nan(sourceFieldValue) or self.replaceWithNull:
                        destination[field] = sourceFieldValue
            elif field_type == "object":
                if self.replaceRecursive and not validator.is_nan(sourceFieldValue):
                    temp_entity = self.entity
                    self.entity = fields[field]
                    if validator.is_nan(destinationFieldValue):
                        destinationFieldValue = fields[field]()
                        destination[field] = destinationFieldValue
                    self.replaceValuesRecursive(destinationFieldValue, sourceFieldValue)
                    self.entity = temp_entity    

    def get_field_type(self, property):
        member = self.entity.__annotations__[property]
        if hasattr(member, "__name__") and member.__name__ in ("str", "int", "float", "bool"):
            return "primitive"
        elif hasattr(member, "__args__"):
            return "list"
        elif hasattr(member, "__annotations__"):
            return "object"

    def is_object_equals(self, source, target):
        match = True
        for fieldName in self.mergeByFields:
            
            sourceObjectFieldValue = self.getFieldValue(fieldName, source)
            targetObjectFieldValue = self.getFieldValue(fieldName, target)
            
            if validator.is_nan(sourceObjectFieldValue):
                match = True
                break

            if validator.is_nan(targetObjectFieldValue):
                match = False
                break    
                
            if sourceObjectFieldValue != targetObjectFieldValue:
                match = False
                break
    
        return match