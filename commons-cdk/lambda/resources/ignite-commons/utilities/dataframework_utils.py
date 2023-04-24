from pyspark.sql.types import *
from utilities import log_utils as log_utils
import pyspark.sql.functions as F

def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    elif f == 'bool' : return BooleanType()
    else: return StringType()

def define_structure(column, format_type):
    try:
        typo = equivalent_type(format_type)
        return StructField(column, typo)
    except: typo = StringType()
    return StructField(column, typo)     

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df, request_context):
    try : 
        columns = list(pandas_df.columns)
        types = list(pandas_df.dtypes)
        struct_list = []
        for column, typo in zip(columns, types): 
            struct_list.append(define_structure(column, typo))
        p_schema = StructType(struct_list)
        return request_context.spark.createDataFrame(pandas_df, p_schema)
    except Exception as e :
        log_utils.print_error_logs('  - Error while converting pandas data frame to spark DF', request_context)
        raise e

def join_dataframe(first_data_frame, second_data_frame, join_by, request_context):
    try:
        for joinby_column in join_by:
            column = joinby_column["target"]
            first_data_frame = first_data_frame.withColumn("temp_key", F.monotonically_increasing_id())
            first_data_frame = first_data_frame.join(second_data_frame,first_data_frame[column] == second_data_frame[column], 
                "left").drop(second_data_frame[column]).sort('temp_key').drop('temp_key')
        return first_data_frame
    except Exception as e :
        log_utils.print_error_logs(' - Error while Join the two data frame!!', request_context)
        raise e
# join fun result will have both common column present in resultant data frame
def equivalent_type_mongo(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    elif f == 'bool' : return BooleanType()
    else: return StringType()
    
def define_structure_mongo(string, format_type):
    try:
        if string=='values' :
            typo = ArrayType(MapType(StringType(),StringType(),True),True)
        else :
            typo = equivalent_type_mongo(format_type)
    except: typo = StringType()
    return StructField(string, typo)


def pandas_to_spark_mongo(targetSystemRoutingId, brand, country, language,pandas_df, spark, logger, x_correlation_id):
    try : 
        columns = list(pandas_df.columns)
        types = list(pandas_df.dtypes)
        struct_list = []
        for column, typo in zip(columns, types) : 
            struct_list.append(define_structure_mongo(column, typo))
        
        p_schema = StructType(struct_list)
        my_schema = StructType([
                    StructField('targetSystemRoutingId', StringType()),
                    StructField('brand', StringType()),
                    StructField('locale', StringType()),
                    StructField('language', StringType()),
                    StructField('mappingTable', ArrayType(StructType(
                        p_schema.fields
                    )))
                ])    
        print(my_schema)
        return spark.createDataFrame(
                [
                    (targetSystemRoutingId.lower(), brand, country, language, pandas_df.to_dict(orient='records'))
                ],
                schema=my_schema
                )
    except Exception as e :
        logger.error('x_correlation_id :'+x_correlation_id+ '  - Error while converting pandas data frame to spark DF')
        logger.error(e)
        raise 

def join_dataframe(first_data_frame, second_data_frame, join_by, request_context):
    try:
        for joinby_column in join_by:
            column = joinby_column["target"] # igniteId
            first_data_frame = first_data_frame.withColumn("temp_key", F.monotonically_increasing_id())
            first_data_frame = first_data_frame.join(second_data_frame,first_data_frame[column] == second_data_frame[column], 
                "left").drop(second_data_frame[column]).sort('temp_key').drop('temp_key')
        return first_data_frame
    except Exception as e :
        log_utils.print_error_logs(' - Error while Join the two data frame!!', request_context)
        raise e