
def getObjectsFromList(results, join_map):
    objects :list = []
    for t in results:
        objectMatch = True
        for field_name in join_map:
            value = getFieldValue(t, field_name)
            if value  != join_map[field_name]:
                objectMatch = False
                break
        
        if objectMatch:
            objects.append(t)
    return objects


def getFieldValue(object, fieldPath) :
    
    if "." in fieldPath:
        dotIndex = fieldPath.index(".")
        currentPropertyName = fieldPath.substring(0, dotIndex)
        remainingPath = fieldPath.substring(dotIndex + 1)

    #     Field currentField = object.getClass().getDeclaredField(currentPropertyName);
    #     currentField.setAccessible(true);
    #     Object currentFieldValue = currentField.get(object);
    #     if (List.class.equals(currentField.getType())) {
    #         if (currentFieldValue == null || ((List<?>) currentFieldValue).isEmpty()) {
    #             return null;
    #         }
    #         return getFieldValue(((List<?>) currentFieldValue).get(0), remainingPath);
    #     } else {
    #         if (currentFieldValue == null) {
    #             return null;
    #         }
    #         return getFieldValue(currentFieldValue, remainingPath);
    #     }
    
    # Field field = object.getClass().getDeclaredField(fieldPath);
    # field.setAccessible(true)
    #return field.get(object)
    return object