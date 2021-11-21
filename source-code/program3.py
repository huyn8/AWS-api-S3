import boto3
import botocore
import logging
import os
import sys
from botocore.exceptions import ClientError
from botocore.errorfactory import ClientError
from datetime import datetime, timezone

"""
	@ Author Huy Nguyen
    @ AWS S3 API
	@ Description: The application recursively traverses the files of a local directory and makes a backup to AWS S3.  
	The program is able to restore from the AWS S3 to local machine as well. The S3 AWS API wil be used to make "back-up" and 
	"restore" requests. Since S3 does not have folders, this program still makes sure that original files and directory
	structures are kept the same.
	The arguments are passed in from the command line to the Python script are as followed:
	For backing up: %backup directory-name bucket-name::directory-name
	For restoring: %restore bucket-name::directory-name directory-name
	The program also uses external authentication (secrets and key ID) to connect and communicate with AWS S3 remotely
	*Note that the arguments entered have to be valid and follow the exact order above for the program to work correctly*
"""

"""
	 Main function to run the program by parsing in the approriate arguments from the command line
	 for either back-up calls or restore calls.

	 @pre: all params are valid, user provided secret credentials are valid for AWS S3 access
	 @post: files are either backed up or restored
	 @param argv: a list of string containing the arguments for either back-up or restore calls
"""
def main(argv):
	if len(argv) < 3:
		print("Please provide correct number of arguments")
		return 
	s3_client = boto3.resource("s3")#initalize s3 client to make API requests to S3 API
	session = boto3.session.Session()#initalize s3 session to make API requests to S3 API

	if argv[0] == "backup":#for backing up files from local machine to S3
		try:
			root_dir = argv[1]
			bucket_name = argv[2].split("::")[0]
			bucket_dir = argv[2].split("::")[1]
			create_bucket_if_needed(bucket_name, s3_client, session)
			print("backing up...")
			back_up(s3_client, session, root_dir, bucket_name, bucket_dir)
		except:
			print("FATAL ERROR: PLEASE ENTER VALID ARGUMENTS")

	elif argv[0] == "restore":#for restoring files from S3 to local machine
		try:
			bucket_name = argv[1].split("::")[0]
			bucket_dir = argv[1].split("::")[1]
			root_dir = argv[2]
			print("restoring...")
			restore(s3_client, session, root_dir, bucket_name, bucket_dir)
		except:
			print("FATAL ERROR: PLEASE ENTER VALID ARGUMENTS")
	else:
		print("No valid operation was chosen")


"""
	 Function to create an S3 bucket if the specified bucket for backing up to S3 does not exist

	 @pre: all specified params are valid, bucket name, s3 client and session are created
	 @post: a new bucket is created if does not exist
	 @params: name of the bucket, s3 client, local session
"""
def create_bucket_if_needed(bucket_name, s3_client, session):
	# create a bucket if needed
	if not s3_client.Bucket(bucket_name) in s3_client.buckets.all():
		try:
			print("Could not find bucket: " + bucket_name + " ->Creating specified bucket...")
			s3_client.create_bucket(Bucket = bucket_name, CreateBucketConfiguration = {"LocationConstraint": session.region_name})
		except (s3_client.meta.client.exceptions.BucketAlreadyExists, botocore.exceptions.ClientError) as err:
			print("Bucket name is taken or invalid, try a different bucket name")
			return None


"""
	 Function to check if uploads are needed for files that are outdated. If a file on S3 is up to date,
	 no unecessary uploads are made.

	 @pre: valid local file path for uploading and S3 file path in the bucket
	 @post: none
	 @params: local file path for uploading and S3 file path in the bucket
"""
def not_up_to_date(current_file_path, s3_file_path):
    s3_file_lmt = ""
	# compare timestamps for up to date or outdated files for backing up purposes
    try:
        s3_file_lmt = s3_file_path.last_modified.timestamp()
    except botocore.exceptions.ClientError as err:
        return True
    current_file_lmt = os.path.getmtime(current_file_path)
    return (s3_file_lmt < current_file_lmt)


"""
	 Function to upload folders and files from a local machine (back-up) to the cloud
	 (AWS S3) using AWS credentials for communicating with S3 APIs remotely. The 
	 orginal structures of the folders and files on the local machine will be 
	 kept the same when uploaded to S3. 
	 *Note that empty folder won't be uploaded or backed up*

	 @pre: all params are valid, bucket name is unique, local root directory 
	 for back-up is valid, s3 client and session are created
	 @post: all files from local machine are uploaded to the cloud
	 @params: s3 client, session, local root directory, bucket name, bucket directory 
"""
def back_up(s3_client, session, root_dir, bucket_name, bucket_dir):
	success = False
	# local files and directory traversal for backing up to S3
	for dir_name, subdir_list, file_list in os.walk(root_dir):
			print('Found directory: %s' % dir_name)
			success = True
			for file_name in file_list:
				print('\t%s' % "Uploading: " + file_name, end = " ")
				full_path = os.path.join(dir_name, file_name).replace("\\", "/")
				s3_full_path = bucket_dir + "/" + full_path
				if not_up_to_date(full_path, s3_client.Object(bucket_name, s3_full_path)):
					s3_client.Bucket(bucket_name).upload_file(full_path, s3_full_path)# upload to S3
					print("---> Uploaded: " + file_name + " successfully")
				else:
					print("---> " + file_name + " is up to date, no new upload is made for this file")
	if not success:
		print("Directory or file: " + root_dir + " does not exist for back-up")


"""
	 Function to restore folders and files from the cloud (AWS S3) to the local machine
     using AWS credentials for communicating with S3 APIs remotely. The orginal structures 
	 of the folders and files restored to the local machine will be kept the same.

	 @pre: all params are valid, bucket name is unique, local root directory 
	 for restoring is valid, s3 client and session are created
	 @post: all files from AWS S3 are downloaded to the local machine in the specified local folder
	 @params: s3 client, session, local directory, bucket name, bucket directory 
"""
def restore(s3_client, session, root_dir, bucket_name, bucket_dir):
	if s3_client.Bucket(bucket_name) not in s3_client.buckets.all():
		print("Invalid bucket: " + bucket_name + " does not exist")
		return None

	s3_bucket = s3_client.Bucket(bucket_name)
	success = False;

	# S3 files and folders (objects) traversal for restoring to local machine
	for obj in s3_bucket.objects.filter(Prefix= bucket_dir):
		target = obj.key if root_dir is None \
            else os.path.join(root_dir, os.path.relpath(obj.key, bucket_dir))
		if not os.path.exists(os.path.dirname(target)):
			os.makedirs(os.path.dirname(target))
			if obj.key[-1] == '/':
				continue
		s3_bucket.download_file(obj.key, target) # download from S3
		success = True

	if success:
		print("Restoring files from bucket: " + bucket_name + " into local directory: " + root_dir + " completed")
	else:
		print("Directory: " + bucket_dir + " in bucket: " + bucket_name + " does not exist")


# program starts here
if __name__ == "__main__":
   main(sys.argv[1:])