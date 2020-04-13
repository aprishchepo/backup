#!/usr/bin/python3

import os, sys
import boto3
import argparse
import logging
from botocore.exceptions import ClientError

s3_client = boto3.client(
    's3',
     endpoint_url="http://" + os.environ['AWS_ENDPOINT'],
)

bucket  = os.environ['AWS_HOST_BUCKET']


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

def upload( project, path ):
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            file_name = os.path.join(root, name)
            object_name = project + "/backup/" + name
            size = object_size_check(bucket, object_name)
            if size is None:
                upload_file(file_name, bucket, object_name)
                print('File', file_name, 'uploaded as', object_name)
            else:
                print('File', file_name, 'exist, please rename it')

if __name__ == "__main__":
    project, path = sys.argv[1], sys.argv[2]
    upload(project, path)