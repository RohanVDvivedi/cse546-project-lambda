from boto3 import client as boto3_client
import face_recognition
import pickle
import csv
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

import logging

logger = logging.getLogger(__name__)

input_bucket = "lambda-input-088664932081"
output_bucket = "lambda-outputbucket-aqls5g3tm1f5"
dynamoDB_table_name = "students"

# Function to read the 'encoding' file
def open_encoding(filename):
	file = open(filename, "rb")
	data = pickle.load(file)
	file.close()
	return data

def face_recognition_handler(event, context):	
	print("Hello")







def upload_csv_to_s3(csv_file_path, bucket_name, object_name):
    # Create an S3 client
    s3 = boto3.client('s3')

    # Upload the CSV file to the S3 bucket
    try:
        response = s3.upload_file(csv_file_path, bucket_name, object_name)
    except Exception as e:
        print(f"Error uploading CSV file: {str(e)}")
        return False

    print(f"CSV file uploaded to S3 bucket {bucket_name} with object name {object_name}")
    return True

def check_dynamoDB(video_name,person_name):
	#abhisheks function
	#check if any name present in dynamoBD by name
	dynamodb = aws_session.resource('dynamodb')
	table = dynamodb.Table(dynamoDB_table_name)
	response = table.query(
        KeyConditionExpression=Key('name').eq(person_name),
    )

	details = response['Items'][0]

	data_csv = [details['name'], details['major'], details['year']]

	csv_file_path = '/tmp/{}.csv'.format(video_name)
	with open(csv_file_path, 'w', encoding='UTF8') as f:
		writer = csv.writer(f)
		writer.writerow(data_csv)

	upload_csv_to_s3(csv_file_path,output_bucket,details)
	
	if response['Count'] > 0:
		print('Name exists in the table.')
		return True
	else:
		print('Name does not exist in the table.')
		return False


	#if present psuh csv data to S3 output bucket.
	#(id,name,year) t S3 out.

	#if pushed return ture, else false