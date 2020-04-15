#!/usr/bin/python3

import os
import boto3
import argparse
import logging
from botocore.exceptions import ClientError

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def object_size_check(bucket, object_name):
    """return the key's size if it exist, else None"""
    try:
        obj = s3_client.head_object(Bucket=bucket, Key=object_name)
        return obj['ContentLength']
    except ClientError as e:
        if e.response['Error']['Code'] != '404':
            raise

s3_client = boto3.client(
    's3',
     endpoint_url="http://" + os.environ['AWS_ENDPOINT'],
)

bucket  = os.environ['AWS_HOST_BUCKET']

parser = argparse.ArgumentParser(description='Put backup files to S3')
parser.add_argument('-j', '--project', default='test', 
                    help='Project name, default=test')
parser.add_argument('-p', '--path', default='backup', 
                    help='Path to files to backup, default=backup')
args = parser.parse_args()

def upload( project, path ):
    for root, files in os.walk(path, topdown=False):
        for name in files:
            file_name = os.path.join(root, name)
            file_size = os.path.getsize(file_name)
            object_name = project + "/backup/" + name
            object_size = object_size_check(bucket, object_name)
            if object_size is None:
                upload_file(file_name, bucket, object_name)
                object_size = object_size_check(bucket, object_name)
                if object_size == file_size:
                    print('File', file_name, file_size, 'uploaded as', object_name, object_size)
                else:
                    print('File', file_name, file_size, 'upload failed', object_size)
            else:
                print('File', file_name, 'exist, please rename it')

if __name__ == "__main__":
    project, path = args.project, args.path
    upload(project, path)