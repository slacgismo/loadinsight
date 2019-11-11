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

    result_list = []

    bucket, prefix = s3_path.split('/', 1)
    continuation_token = None
    # get all objects name in current dir
    while True:
        response = None
        request_kwargs = {
            'Bucket':bucket,
            'Delimiter':'/'
        }
        # list sub dir
        if prefix != '':
            request_kwargs['Prefix'] = prefix

        # get next trunk
        if continuation_token:
            request_kwargs['ContinuationToken'] = continuation_token

        response = s3_client.list_objects_v2(**request_kwargs)

        # directory not found
        if 'Contents' not in response and 'CommonPrefixes' not in response:
            return None

        # extract file
        if 'Contents' in response:
            for obj in response['Contents']:
                suffix = obj['Key'] if prefix == '' else obj['Key'].split(prefix, 1)[1]
                if suffix == '':
                    continue
                if '/' not in suffix:
                    result_list.append(suffix)

        # extract dir
        if 'CommonPrefixes' in response:
            for obj in response['CommonPrefixes']:
                dir_levels = obj['Prefix'].split('/')
                suffix = dir_levels[-2] + '/' if len(dir_levels) > 2 else obj['Prefix']
                result_list.append(suffix)

        # at the end of the list
        if not response['IsTruncated']:  
            break
        continuation_token = response['NextContinuationToken']
        
    return list(set(result_list))


if __name__ == "__main__":
    # upload_file('a.txt', 'loadinsight-bucket', 'a/b/a.txt')
    # file_name = 'loadinsight-bucket/a.txt'

    print(list_files_in_dir('loadinsight-bucket/'))
    # for f in list_files_in_dir(dir_name):
    #     if not f.endswith('/'):
    #         print(dir_name + f)
    #         print(read_file_binary(dir_name + f))