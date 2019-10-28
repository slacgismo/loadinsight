import os
import json
import uuid
import boto3
import logging
import hashlib
import botocore
import pandas as pd
from load_model.settings import base
from .file_type_enum import SupportedFileType, SupportedFileReadType


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class ArtifactDataManager(object):
    """
    A Data Manager class responsible for reading and writing data from
    multiple sources and of various types. This class ought to be able 
    to read/write csv, xls and json files from the local FS and from the 
    a remote S3 bucket.
    """
    supported_file_types = [SupportedFileType.CSV, SupportedFileType.JSON, SupportedFileType.XLS, SupportedFileType.XLSX]

    def _parse_extension(self, filename):
        name, extension = os.path.splitext(filename)
        extension = extension.lower()
        
        if extension not in [item.value for item in self.supported_file_types]:
            err_msg = (f'File {filename} with extension {extension} is not a supported type.'
                      f'Please choose a file with one the following types: {self.supported_file_types}')
            logger.exception(err_msg)
            raise TypeError(err_msg)

        return extension

    def _read_config(self, filename):
        # currently we only support configuration files that are json
        extension = self._parse_extension(filename)
        if extension != SupportedFileType.JSON.value:
            raise ValueError('We currently do not support configurations files that are not .json')

        with open(f'{base.CONFIG_PATH}/{filename}') as json_file:
            config_data = json.load(json_file)
            return config_data

    def _read_file(self, filename):
        extension = self._parse_extension(filename)
        full_local_file_path = f'{base.LOCAL_PATH}/{filename}'
        
        # let's ensure first that the file exists locally
        if not self.does_file_exist(filename):
            # otherwise, let's try to grab it from s3
            logger.info(f'Attempting to load {filename} from S3')
            self._read_from_s3(filename)
        
        # if no exception is thrown, we can safely attempt to read the file
        try:            
            if extension == SupportedFileType.CSV.value:
                df = pd.read_csv(full_local_file_path)
            
            elif extension == SupportedFileType.JSON.value:
                df = pd.read_json(full_local_file_path, typ='series')
            
            elif extension in [SupportedFileType.XLS.value, SupportedFileType.XLSX.value]:
                df = pd.read_excel(full_local_file_path)
        
        except FileNotFoundError as fe:
            logger.exception(f'Could not find the file {filename} in the local system at local_data/')
            raise fe
        
        return df

    def _read_from_s3(self, filename):
        temp_filename = str(uuid.uuid4())
        s3 = boto3.client('s3')
        try:
            s3.download_file(base.REMOTE_PATH, f'{filename}', temp_filename)
            os.rename(temp_filename, f'{base.LOCAL_PATH}/{filename}')
        except botocore.exceptions.ClientError as e:
            # try:
            #     s3.download_file(base.REMOTE_PATH, f'ceus/{filename}', temp_filename)
            #     os.rename(temp_filename, f'{base.LOCAL_PATH}/{filename}')
            # except botocore.exceptions.ClientError as e:
            #     try:
            #         s3.download_file(base.REMOTE_PATH, f'mix/{filename}', temp_filename)
            #         os.rename(temp_filename, f'{base.LOCAL_PATH}/{filename}')
            #     except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.exception(f'File {filename} does not exist in S3 under {base.REMOTE_PATH}')
            else:
                logger.exception(e)
            raise e

    def save_data(self, filename, df):
        extension = self._parse_extension(filename)
        full_local_file_path = f'{base.LOCAL_PATH}/{filename}'
            
        if extension == SupportedFileType.CSV.value:
            df.to_csv(full_local_file_path)
        
        elif extension == SupportedFileType.JSON.value:
            df.to_json(full_local_file_path)
        
        elif extension in [SupportedFileType.XLS.value, SupportedFileType.XLSX.value]:
            df.to_excel(full_local_file_path)

    def does_file_exist(self, filename):
        return os.path.isfile(f'{base.LOCAL_PATH}/{filename}')

    def load_data(self, data_files):
        data_dict = {}

        for entry in data_files:
            filename = entry['name']
            file_read_type = entry['read_type']

            logger.info(f'Reading {filename}')

            if file_read_type is SupportedFileReadType.DATA:
                data_dict[filename] = self._read_file(filename)
            
            elif file_read_type is SupportedFileReadType.CONFIG:
                data_dict[filename] = self._read_config(filename)
            
            else:
                raise ValueError('Unsupported file read type')

        return data_dict

    def delete_file(self, filename):
        os.remove(f'{base.LOCAL_PATH}/{filename}')

    def get_data_frame_hash(self, df):
        if isinstance(df, pd.DataFrame):
            return hashlib.sha1(df.to_csv().encode()).hexdigest()
        raise TypeError('Artifact Data Manager expected a DataFrame during hashing and did not receive it')

    def check_file_contents_hash(self, filename):
        """
        Return the SHA-1 hash of the file contents
        """
        h = hashlib.sha1()
        with open(f'{base.LOCAL_PATH}/{filename}', 'rb') as file:
            chunk = 0
            while chunk != b'':
                chunk = file.read(1024)
                h.update(chunk)
        return h.hexdigest()
