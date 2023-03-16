import pandas as pd
import boto3

def trans(bucket):
    s3 = boto3.resource('s3')
    bucket_name = s3.Bucket(bucket)
    for obj in bucket_name.objects.all():
        body = obj.get()['Body'].read()
        df = pd.read_csv(obj.get()['Body'])
    print("Body : ", body)

    data = df.to_dict(orient="records")

    return data