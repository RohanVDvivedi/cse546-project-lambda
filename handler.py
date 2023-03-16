from boto3 import client as boto3_client
import boto3
import face_recognition
import pickle
import os
import shutil
import pathlib

input_bucket = "lambda-input-088664932081"
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
	if not os.path.exists(os.getcwd() + "/temp"):
		os.makedirs(os.getcwd() + "/temp")
	s3_client.download_file(input_bucket, key, os.getcwd() + "/temp")

	# convert the video file to set of frames
	os.system("ffmpeg -i " + os.getcwd() + "/temp/" + key + " -r 1 " + os.getcwd() + "/temp/image-%3d.jpeg")

	# load images of all known people into face_recognition library
	model = open_encoding("/home/app/encoding")
	model_encoding = model['encoding']
	names_list = model['name']

	# loop over the frames of the ffmpeg extracting each frames
	done = False
	for frame_path in list(pathlib.Path(os.getcwd() + "/temp").glob('*.jpeg')) :
		frame = face_recognition.load_image_file(frame_path)
		frame_encoding = face_recognition.face_encodings(frame)[0]
		results = face_recognition.compare_faces(model_encoding, frame_encoding)

		for i in range(0, len(results)) :
			if(results[i]) :
				check_dynamoDB(names_list[i])
				done  = True
				break

		if(done) :
			break
	
	# delete all frames and images
	shutil.rmtree(os.getcwd() + "/temp")