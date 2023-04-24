"""
Wrapper over Mongo, class to prepare all the data based upon lifecycle of feed,
apply any aggregation of result retuned after aggregation rules.

"""
import random
import json
from enum import Enum
import models.constants as model_constant
import utilities.constant as constant
import utilities.iterator_utils as iterator_utils
import utilities.log_utils as log_utils
import utilities.mongo_utils as mongo_utils
import utilities.prop_reader as prop_reader
import utilities.validator as validator
from commons.aggregator_result import AggregatorResult, Status
from exception.repository_operation_exception import RepositoryOperationException
from utilities.ignite.feed_runtime_context import FeedRuntimeContext
from utilities.serializers.object_to_json import IgniteJsonEncoder
from models.PredicateJoinCondition import PredicateJoinCondition

class RepositoryAction(Enum):
    DROP_INDEX = "Drop index over collection",
    MARK_DOCUMENTS_FOR_DELETE = "Mark documents for delete(soft delete)",
    DELETE_DOCUMENTS = "Delete marked documents (hard delete)"
    ADD_DOCUMENTS = "Add new documents"
    RECREATE_INDEXES = "Recreate the deleted indexes"

feed_runtime_context = FeedRuntimeContext.get_instance()
    

def prepare_mongo():
    try:
        
        for feed_file in feed_runtime_context.feed_file_aggregation_results_map.keys():

            aggregation_results: AggregatorResult = feed_runtime_context.feed_file_aggregation_results_map.get(
                feed_file)

            if aggregation_results == None:
                log_utils.print_info_logs(" AggregatorResult for feedFile:{} is None".format(feed_file))
                continue
            
            for index, aggregated_result in enumerate(aggregation_results):

                is_invalid_aggregated_result = ((aggregated_result == None) or
                                                (not hasattr(aggregated_result, "parserResult")) or
                                                (iterator_utils.is_empty(aggregated_result.parserResult.results)))

                if is_invalid_aggregated_result:
                    log_utils.print_debug_logs(" Aggregated results for {} no found, no db operations required".format(aggregated_result.collection))
                    continue

                log = "start saving results of feed file: {feed_file} on collection '{collection}'" \
                        .format(feed_file=feed_file, collection=aggregated_result.collection)        
                
                log_utils.print_info_logs(log)

                previous_step = None
                current_step = None
                delete_documents = None

                try:
                    # pre-step: drop all existing indexes
                    current_step = RepositoryAction.DROP_INDEX
                    drop_existing_indexes(aggregated_result)

                    # Step 1. mark entries for delete
                    previous_step = RepositoryAction.DROP_INDEX
                    current_step = RepositoryAction.MARK_DOCUMENTS_FOR_DELETE
                    mark_entries_to_delete(aggregated_result)

                    # Step 2. delete marked entries
                    previous_step = RepositoryAction.MARK_DOCUMENTS_FOR_DELETE
                    current_step = RepositoryAction.DELETE_DOCUMENTS
                    delete_documents = delete_mongo_entries(aggregated_result)

                    # Step 3. add new entries
                    previous_step = RepositoryAction.DELETE_DOCUMENTS
                    current_step = RepositoryAction.ADD_DOCUMENTS
                    add_mongo_entries(aggregated_result)

                    # post step: recreated indexes
                    previous_step = RepositoryAction.ADD_DOCUMENTS
                    current_step = RepositoryAction.RECREATE_INDEXES
                    create_indexes(aggregated_result)

                except RepositoryOperationException as e:
                    aggregated_result.status = Status.FAIELD
                    log_utils.print_error_logs("{exception} while saving results of {feed_file}, cheking whether rollback required".
                                               format(exception=e.message, feed_file=feed_file))
                    rollback(delete_documents, aggregated_result, current_step, previous_step)
                    raise e

                # after all db operations complete on each feed file aggregated result
                # set the status to success on aggregated result
                aggregated_result.status = Status.SUCCESS

                log = "completed saving of results for feed file: {feed_file} on collection '{collection}v2'" \
                        .format(feed_file=feed_file, collection=aggregated_result.collection)
                log_utils.print_info_logs(log)              

        print("completed mongo operations")
    except Exception as e:
        aggregated_result.status = Status.FAIELD
        log_utils.print_error_logs(str(e))
        raise RepositoryOperationException(str(e))


def mark_entries_to_delete(aggregated_result: AggregatorResult):

    brand_country_language_prefix = FeedRuntimeContext.get_instance().brand_country_language_prefix
    
    collection_name_key = "database_collection_{}".format(
        aggregated_result.collection)

    collection_name = prop_reader.get_prop(
        constant.mongo_section, collection_name_key)

    log_utils.print_info_logs(
        " starting query to mark records eliglible to delete")

    # first build query to get documents eligible to mark as delete(soft delete)
    query = build_basic_query(aggregated_result)

    document_count = mongo_utils.count(query, collection_name)
    if document_count == 0:
        log_utils.print_info_logs("No documents found in collection: '{}', nothing eligible for delete"
                                  .format(collection_name))
        return

    log_utils.print_info_logs("{}: found: '{}' documents in collection: '{}', eligible for delete"
                              .format(brand_country_language_prefix, str(document_count), collection_name))

    # add soft-delete flag on documents
    attribute = {"$set": {"softDel": "1"}}

    mongo_utils.update_many(query, attribute, collection_name)

    log_utils.print_info_logs(" completed marking of eliglible records for delete in collection: ".format(
        collection_name))


def add_mongo_entries(aggregated_result):

    brand_country_language_prefix = FeedRuntimeContext.get_instance().brand_country_language_prefix+"_"+str(random.randint(1111,9999))
    collection_name_key = "database_collection_{}".format(
        aggregated_result.collection)
    
    collection_name = prop_reader.get_prop(constant.mongo_section, collection_name_key)
    documents = json.loads(json.dumps(aggregated_result.parserResult.results, cls=IgniteJsonEncoder))

    log_utils.print_info_logs(" starting add documents query")
    status = mongo_utils.insert_many(documents, collection_name)
    # status = mongo_utils.insert_one(documents, collection_name)
    
    if not status.acknowledged:
        # if status is not 200 then throw exception
        raise RepositoryOperationException("Exception while saving documents to db")

    log_utils.print_info_logs(" {}: added: {} documents in collection: '{}'"
        .format(brand_country_language_prefix, len(aggregated_result.parserResult.results), collection_name))

def delete_mongo_entries(aggregated_result):
    
    brand_country_language_prefix = FeedRuntimeContext.get_instance().brand_country_language_prefix

    collection_name_key = "database_collection_{}".format(aggregated_result.collection)

    collection_name = prop_reader.get_prop(
        constant.mongo_section, collection_name_key)

    log_utils.print_info_logs(
        " starting query to hard delete records which were eliglible to delete")

    # first build query to get documents where were eligible to delete in previous step(soft delete)
    query = build_basic_query(aggregated_result)

    # include softDel flag
    query.update({"softDel": "1"})

    document_count = mongo_utils.count(query, collection_name)
    if document_count == 0:
        log_utils.print_info_logs("{}: No documents found in collection: '{}', nothing eligible for hard delete"
                                  .format(brand_country_language_prefix, collection_name))
        return

    log_utils.print_info_logs("{}: found: '{}' documents in collection: '{}', eligible for hard delete"
                              .format(brand_country_language_prefix, str(document_count), collection_name))

    """
    hold the documents which we are about the delete
    if anything goes wrong when adding the new documents, 
    insert these old documents back
    typically this hack is for rollback
    """
    deleted_documents = mongo_utils.find_many(
        query, collection_name)

    # hard delete
    mongo_utils.delete_many(query, collection_name)
    log_utils.print_info_logs("{}: completed hard delete of records for collection: {}".format(brand_country_language_prefix, collection_name))

    return deleted_documents


def build_basic_query(aggregated_result: AggregatorResult):
    
    query = { 
             "brands": 
                {"$in": [feed_runtime_context.brand]},
             "country": feed_runtime_context.country
            }

    """
    if current configuration supports multiple languages, then get the language
    from current current
    """
    current_configuration = FeedRuntimeContext.get_instance().current_configuration
    language = get_language(current_configuration)
    
    if language is not None:
        query["language"] = language
     
    lifecycleQuery: str = aggregated_result.lifecycleQuery
    
    if validator.is_nan(lifecycleQuery):
        return query

    predicateJoinType: PredicateJoinCondition = aggregated_result.predicateJoinCondition

    if predicateJoinType == PredicateJoinCondition.AND:
        query_with_and = {"$and": []}
        query_with_and["$and"].append(query)
        query_with_and["$and"].append(json.loads(aggregated_result.lifecycleQuery))
        return query_with_and

    if predicateJoinType == PredicateJoinCondition.OR:
        query_with_or = {"$or": []}
        query_with_or["$or"].append(query)
        query_with_or["$or"].append(json.loads(aggregated_result.lifecycleQuery))
        return query_with_or
    
    
def drop_existing_indexes(aggregated_result):
    collection_name_key = "database_collection_{}".format(aggregated_result.collection)
    collection_name = prop_reader.get_prop(constant.mongo_section, collection_name_key)

    if aggregated_result.collection not in model_constant.COLLECTIONS_INDEXES:
        log_utils.print_info_logs(" db indexes not avalible for collection: {}".format(collection_name))
        return

    mongo_utils.drop_indexes(collection_name)

def create_indexes(aggregated_result):
    
    collection_name_key = "database_collection_{}".format(
        aggregated_result.collection)

    collection_name = prop_reader.get_prop(
        constant.mongo_section, collection_name_key)

    if aggregated_result.collection not in model_constant.COLLECTIONS_INDEXES:
        log_utils.print_info_logs(" No indexes avalible for creation on collection: {}".format(
            collection_name))
        return

    indexes = model_constant.COLLECTIONS_INDEXES.get(aggregated_result.collection)
    for index in indexes:
        index_list = []
        if index["type"] == "SINGLE":
            index_tuple = (index["index_name"], index["specifier"])
            index_list.append(index_tuple)
            
        if index["type"] == "COMPOSITE":
            for sub_index in index["index_name"]:
                index_tuple = (sub_index, index["specifier"])
                index_list.append(index_tuple)
        mongo_utils.create_index(collection_name, index_list)
    log_utils.print_info_logs('Finished adding indexes')

"""
If the repository action fail at any step then start the rollback process.
The rollback process should be start only when the old documents are deleted and 
the exception was thrown between delete documents and add documents step
"""
def rollback(delete_documents, aggregated_result, current_step, previous_step):
    
    if previous_step == None and current_step == RepositoryAction.DROP_INDEX:
        log_utils.print_debug_logs(
            " failed before dropping index, no rollback required")
        return

    if previous_step == RepositoryAction.DROP_INDEX and current_step == RepositoryAction.MARK_DOCUMENTS_FOR_DELETE:
        log_utils.print_debug_logs(
            " failed before soft delete of documents index, recreating the dropped indexes")
        create_indexes(aggregated_result)
        log_utils.print_debug_logs("recreated the dropped indexes")
        return
    
    if previous_step == RepositoryAction.MARK_DOCUMENTS_FOR_DELETE and current_step == RepositoryAction.ADD_DOCUMENTS: 
        log_utils.print_debug_logs(
            " failed before inserting new documents, restoring the previous documents")
        # first build query to get documents where were eligible to delete in previous step(soft delete)
        collection_name_key = "database_collection_{}".format(aggregated_result.collection)
        collection_name = prop_reader.get_prop(constant.mongo_section, collection_name_key)
        query = build_basic_query(aggregated_result)
        mongo_utils.delete_many(query, collection_name)
        mongo_utils.insert_many(delete_documents)
        return

    if previous_step == RepositoryAction.ADD_DOCUMENTS and current_step == RepositoryAction.RECREATE_INDEXES:
        log_utils.print_debug_logs(
            " failed before recreating indexes, attempting one more time to create index")
        create_indexes(aggregated_result)
        log_utils.print_debug_logs("recreated the dropped indexes successfully")
        

def get_language(current_configuration):
    
    language = None 
    
    # pull the language from configuration directly if exist
    if "language" in current_configuration and not validator.is_nan(current_configuration["language"]):
        language = current_configuration["language"]
        
    # market supports multiple languages, then pull it from:
    if "multilanguage" in current_configuration and current_configuration["multilanguage"]:
        if "language" in current_configuration and not validator.is_nan(current_configuration["language"]):
            language = current_configuration["language"]
    
    if FeedRuntimeContext.get_instance().brand_country_language_prefix == None:
        if language is not None:
            FeedRuntimeContext.get_instance().brand_country_language_prefix = "{}_{}_{}". \
                    format(feed_runtime_context.brand, feed_runtime_context.country, language)
        else:
            FeedRuntimeContext.get_instance().brand_country_language_prefix = "{}_{}". \
                    format(feed_runtime_context.brand, feed_runtime_context.country)    

    return language 


