from utilities import log_utils as log_utils

# this will create a map of the model per language 
# pass any model say Label and type to get the labels type list by language
#
def get_labels_per_language(parser_result_list,type,request_context):
    try:
        model_per_language = {} # { language: ["type1", "type2"]}
        for parser_result in parser_result_list:
            model_type_list=[]
            for model in parser_result.results:
                if(hasattr(model, type) and type== model.type):
                    language= model.language if hasattr(model, "language") else None
                    originalId=model.originalId if hasattr(model, "originalId") else None
                    if language and originalId:                        
                        if(language in model_per_language):
                            model_type_list=model_per_language[language]
                        if originalId not in model_type_list:                           
                            model_type_list.append(originalId)
                            dict_obj={language:model_type_list}
                            model_per_language.update(dict_obj)
        return model_per_language
    except Exception as e:
        log_utils.print_error_logs(str(e), request_context)
        raise e    

