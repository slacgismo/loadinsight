import logging
import boto3
import json
import uuid
import os
from botocore.exceptions import ClientError

s3_client = boto3.client('s3')


def delete_file(file_name, bucket):
    response = s3_client.delete_object(
        Bucket=bucket,
        Key=file_name
    )



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


def read_file_binary(path):
    '''Download file from s3

    :param file_name: File to download, the name you save
    :param bucket: Bucket to download from
    :param object_name: S3 object name, the whole path
    '''
    temp_filename = str(uuid.uuid4())
    bucket, object_name = path.split('/', 1)
    try:
        s3_client.download_file(bucket, object_name, temp_filename)
    except ClientError as e:
        logging.error(e)
        return None
    
    file = None
    with open(temp_filename, "rb") as fp:
        file = fp.read()
        fp.close()
        os.remove(temp_filename)
    return file


def list_files_in_dir(s3_path):
    '''List file and dir in a s3 dir

    :param s3_path: '<bucket-name>/dir/sub-dir/'
    '''
    # not a directory format
    if not s3_path.endswith('/'):
        s3_path += '/'

    bucket, prefix = s3_path.split('/', 1)
    response = None
    if prefix == '':
        # list root dir of bucket
        response = s3_client.list_objects(
            Bucket=bucket
        )
    else:
        # list sub dir
        response = s3_client.list_objects(
            Bucket=bucket,
            Prefix=prefix
        )

    # directory not found
    if 'Contents' not in response:
        return None

    content_list = response['Contents']
    result_list = []
    dir_set = set()

    # extract file and dir
    for obj in content_list:
        suffix = obj['Key'] if prefix == '' else obj['Key'].split(prefix, 1)[1]
        if suffix == '':
            continue
        if '/' not in suffix:
            result_list.append(suffix)
        else:
            dir_set.add(suffix.split('/')[0] + '/')

    result_list.extend(list(dir_set))
    return result_list


if __name__ == "__main__":
    # upload_file('a.txt', 'loadinsight-bucket', 'a/b/a.txt')
    file_name = 'loadinsight-bucket/a.txt'
    delete_file('a.txt', 'loadinsight-bucket')
    # for f in list_files_in_dir(dir_name):
    #     if not f.endswith('/'):
    #         print(dir_name + f)
    #         print(read_file_binary(dir_name + f))