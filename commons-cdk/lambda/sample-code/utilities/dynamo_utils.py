import boto3


def save_to_dynamo(data_document):

    dynamo_client = boto3.resource('dynamodb')

    product_table = dynamo_client.Table('test')
    for i in data_document:
        dynamo_save_result = product_table.put_item(Item=i)

    return True