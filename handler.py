from boto3 import client as boto3_client
import boto3
import face_recognition
import pickle
import csv
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import os
import shutil
import pathlib

dynamoDB_table_name = "students"
input_bucket = "inputbucket-088664932081"
output_bucket = "lambda-output-cse546"

aws_session = boto3.Session(
	aws_access_key_id = "",
    aws_secret_access_key = "",
    region_name="us-east-1"
)

# Function to read the 'encoding' file
def open_encoding(filename):
	file = open(filename, "rb")
	data = pickle.load(file)
	file.close()
	return data

def face_recognition_handler(event, context):
	# extract key of the file pushed
	key = event['Records'][0]['s3']['object']['key']

	# save the video file to cwd/temp
	s3_client = aws_session.client('s3')
	if not os.path.exists("/tmp/temp"):
		os.makedirs("/tmp/temp")
	s3_client.download_file(input_bucket, key, "/tmp/temp")

	# convert the video file to set of frames
	os.system("ffmpeg -i /tmp/temp/" + key + " -r 1 /tmp/temp/image-%3d.jpeg")

	# load images of all known people into face_recognition library
	model = open_encoding("/home/app/encoding")
	model_encoding = model['encoding']
	names_list = model['name']

	# loop over the frames of the ffmpeg extracting each frames
	done = False
	for frame_path in list(pathlib.Path("/tmp/temp").glob('*.jpeg')) :
		frame = face_recognition.load_image_file(frame_path)
		frame_encoding = face_recognition.face_encodings(frame)[0]
		results = face_recognition.compare_faces(model_encoding, frame_encoding)

		for i in range(0, len(results)) :
			if(results[i]) :
				check_dynamoDB(key, names_list[i])
				done = True
				break

		if(done) :
			break
	
	# delete all frames and images
	shutil.rmtree("/tmp/temp")

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