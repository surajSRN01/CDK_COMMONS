import boto3
import utilities.constant as const
import utilities.log_utils as log_utils
import utilities.prop_reader as prop_reader
from exception.repository_operation_exception import RepositoryOperationException
from pymongo import MongoClient
from utilities.ignite.feed_runtime_context import FeedRuntimeContext


feed_runtime_context = FeedRuntimeContext.get_instance()
    
def get_mongo_client():
    type = feed_runtime_context.clientType
    log_utils.print_info_logs(' creating mongo_client ')
    host_prop = prop_reader.get_prop(const.mongo_section, 'parameter_store_'+type+'_host')
    database = prop_reader.get_prop(const.mongo_section, 'database_'+type)
 
    ssm = boto3.client(const.SSM)
    host_dict = ssm.get_parameter(Name=host_prop, WithDecryption=True)
    host = host_dict['Parameter']['Value']
    port="27017"

    host_list = host.split(",")
    mongo_uri = get_mongo_uri(database, host_list, port)
    mongo_uri = 'mongodb://'+host+':'+port if mongo_uri is None else mongo_uri
        
    log_utils.print_info_logs(' mongodb uri list: '+str(mongo_uri))    
    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client[database]
    return mongo_db

def get_mongo_uri(database, host_list, port):
    for host in host_list:
        mongo_uri = 'mongodb://'+host+':'+port
        try:
            client = MongoClient(mongo_uri+'/'+database,serverSelectionTimeoutMS=1000)
            ismaster = client.admin.command('ismaster')
            if (ismaster['ismaster']):
                log_utils.print_info_logs('primary mongo uri : '+str(mongo_uri))
                return mongo_uri
            else:
                log_utils.print_info_logs('mongo uri : '+str(mongo_uri)+ " is not primary")
        except Exception as e:
            log_utils.print_error_logs("error while connecting MongoDB with URI : "+str(mongo_uri))
            log_utils.print_error_logs(str(e))

def find_one(query, collection):
    try:
        log_utils.print_info_logs(' executing find_one document on collection : '+collection)
        mongo_client = get_mongo_client()
        mongo_collection = mongo_client[collection]
        documents = mongo_collection.find_one(query)
        if(len(documents) == 0):
            log_utils.print_info_logs(" excuted find_one from collection: {}".format(collection))
        elif(len(documents) > 1):     
            log_utils.print_info_logs(" excuted find_one from collection: {}".format(collection))
        return documents[0]
    except Exception as e :
        raise RepositoryOperationException("Exeption in find_one mongo call over collection: {collection}, error: {error}"
            .format(collection=collection, error=str(e))) 

def find_many(query, collection):
    try :
        log_utils.print_info_logs(' executing find_many document on collection : '+collection)
        mongo_client = get_mongo_client()
        mongo_collection = mongo_client[collection]
        documents = mongo_collection.find(query)
        log_utils.print_info_logs(" excuted find_many from collection: {}".format(collection))
        return documents
    except Exception as e :
        raise RepositoryOperationException("Exeption in find_many mongo call over collection: {collection}, error: {error}"
            .format(collection=collection, error=str(e))) 

def update_many(query, attributes, collection):
    try :
        log_utils.print_info_logs(" executing update_many from collection: {}".format(collection))
        mongo_client = get_mongo_client()
        mongo_collection = mongo_client[collection]
        response = mongo_collection.update_many(query, attributes, upsert=True)
        log_utils.print_info_logs(" excuted update_many from collection: {}".format(collection))
        return response
    except Exception as e :
        raise RepositoryOperationException("Exeption in update_many mongo call over collection: {collection}, error: {error}"
            .format(collection=collection, error=str(e))) 

def count(query, collection):
    try :
        log_utils.print_info_logs(" executing count query on collection : {}".format(collection))
        mongo_client = get_mongo_client()
        mongo_collection = mongo_client[collection]
        documents_count = mongo_collection.count_documents(query)
        log_utils.print_info_logs(" executed count query on collection : {}".format(collection))
        return documents_count
    except Exception as e :
        raise RepositoryOperationException("Exeption in count mongo call over collection: {collection}, error: {error}"
            .format(collection=collection, error=str(e))) 

def insert_many(documents, collection):
    try :
        log_utils.print_info_logs(" executing insert_many query on collection : {}".format(collection))
        mongo_client = get_mongo_client()
        mongo_collection = mongo_client[collection]
        status = mongo_collection.insert_many(documents)
        log_utils.print_info_logs(" executed insert_many query on collection : {}".format(collection))
        return status
    except Exception as e :
        raise RepositoryOperationException("Exeption in insert_many mongo call over collection: {collection}, error: {error}"
            .format(collection=collection, error=str(e))) 

def insert_one(documents, collection):
    try :
        log_utils.print_info_logs(" executing insert_many query on collection : {}".format(collection))
        mongo_client = get_mongo_client()
        mongo_collection = mongo_client[collection]
        for i in documents:
            status = mongo_collection.insert_one(i)
        log_utils.print_info_logs(" executed insert_many query on collection : {}".format(collection))
        return status
    except Exception as e :
        raise RepositoryOperationException("Exeption in insert_many mongo call over collection: {collection}, error: {error}"
            .format(collection=collection, error=str(e))) 
    
def delete_many(query, collection,  ):
    try :
        log_utils.print_info_logs(" executing delete_many query on collection : {}".format(collection))
        mongo_client = get_mongo_client( )
        mongo_collection = mongo_client[collection]
        status = mongo_collection.delete_many(query)
        log_utils.print_info_logs(" executed delete_many query on collection : {}".format(collection))
        return status
    except Exception as e :
        raise RepositoryOperationException("Exeption in delete_many mongo call over collection: {collection}, error: {error}"
            .format(collection=collection, error=str(e))) 

def drop_indexes(collection):
    try :
        log_utils.print_info_logs(" starting to drop indexes on collection : {}".format(collection))
        mongo_client = get_mongo_client()
        mongo_collection = mongo_client[collection]
        status = mongo_collection.drop_indexes()
        log_utils.print_info_logs(" dropped all indexes on collection : {}".format(collection))
        return status
    except Exception as e :
        raise RepositoryOperationException("Exeption in drop_indexes mongo call over collection: {collection}, error: {error}"
            .format(collection=collection, error=str(e))) 

def create_index(collection, index):
    try :
        log_utils.print_info_logs(" starting to create index on collection : {}".format(collection))
        mongo_client = get_mongo_client()
        mongo_collection = mongo_client[collection]
        status = mongo_collection.create_index(index)
        log_utils.print_info_logs(" added index on collection : {}".format(collection))
        return status
    except Exception as e :
        raise RepositoryOperationException("Exeption in create_index mongo call over collection: {collection}, error: {error}"
            .format(collection=collection, error=str(e))) 
    
def create_indexes(collection, indexes):
    try :
        log_utils.print_info_logs(" starting to create index on collection : {}".format(collection))
        mongo_client = get_mongo_client()
        mongo_collection = mongo_client[collection]
        status = mongo_collection.create_indexes(str(indexes))
        log_utils.print_info_logs(" added index on collection : {}".format(collection))
        return status
    except Exception as e :
        raise RepositoryOperationException("Exeption in create_index mongo call over collection: {collection}, error: {error}"
            .format(collection=collection, error=str(e))) 