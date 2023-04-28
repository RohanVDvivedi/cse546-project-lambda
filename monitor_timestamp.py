import os
import boto3
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

aws_session = boto3.Session(
    aws_access_key_id="AKIARJJGXP3YRNZOPD72",
    aws_secret_access_key="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",
    region_name="us-east-1"
)

s3_client = aws_session.client('s3')
lambda_client = aws_session.client('lambda')

def main():
    tPrev = None
    while True:
        response = s3_client.list_objects_v2(Bucket='inputbucketproject3new')
        if 'Contents' in response:
            objectKeys = response['Contents']
            # filter out objects with last modified time greater than or equal to tPrev
            objectKeys = [obj for obj in objectKeys if (tPrev == None or obj['LastModified'] > tPrev)]
            if len(objectKeys) == 0:
                time.sleep(2)  # Wait for 2 seconds before checking again
                continue
            # sort the objects by last modified time in descending order, latest last_modified will be first
            objectKeys = sorted(objectKeys, key=lambda obj: obj['LastModified'], reverse=True)
            tPrev = objectKeys[0]['LastModified']

            # single threaded
            for objKey in objectKeys:
                # Invoke the Lambda function with the object key as input
                lambda_client.invoke(FunctionName='lambdaforproject3', Payload='{"key":"' + objKey['Key'] + '"}')
            
            # multithreaded
            #keys = [objKey['Key'] for objKey in objectKeys]
            #with ThreadPoolExecutor(max_workers = 5) as executor:
                #executor.map(lambda key : lambda_client.invoke(FunctionName='lambdaforproject3', Payload='{"key":"' + key + '"}'), keys)

if __name__ == '__main__':
    main()
