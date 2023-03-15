from boto3 import client as boto3_client
import face_recognition
import pickle

input_bucket = "lambda-input-088664932081"
output_bucket = "lambda-output-cse546"

# Function to read the 'encoding' file
def open_encoding(filename):
	file = open(filename, "rb")
	data = pickle.load(file)
	file.close()
	return data

def face_recognition_handler(event, context):
	# upload images of all known people into face_recognition library
	
	# extract key of the file pushed
	key = event['Records'][0]['s3']['object']['key']
	print("Hello + " + key)

	# grab video file from the bucket using the key

	# loop over the frames of the ffmpeg extracting each frames

		# pass the frame to face_recognition library to extract names and location of all people identitied

		# pass the list of names to abhishek's function

		# if abhishek's function returns true, quit the loop