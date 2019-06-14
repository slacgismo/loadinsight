import os
import json
import logging
import pandas as pd
from settings import base
from generics.file_type_enum import SupportedFileType, SupportedFileReadType


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

        with open(filename) as json_file:  
            config_data = json.load(json_file)
            return config_data

    def _read_file(self, filename):
        extension = self._parse_extension(filename)
        full_local_file_path = f'{base.LOCAL_PATH}/{filename}'
        
        try:            
            if extension == SupportedFileType.CSV.value:
                df = pd.read_csv(full_local_file_path)
            
            elif extension == SupportedFileType.JSON.value:
                df = pd.read_json(full_local_file_path, typ='series')
            
            elif extension in [SupportedFileType.XLS.value, SupportedFileType.XLSX.value]:
                df = pd.read_excel(full_local_file_path)
        
        except FileNotFoundError as fe:
            logger.exception(f'Could not find the file {filename} in the local system at local_data/')
            logger.info(f'Attempting to load {filename} from S3')
            raise fe
        
        return df

    def _write_file(self, filename, df):
        extension = self._parse_extension(filename)
        full_local_file_path = f'{base.LOCAL_PATH}/{filename}'            
            
        if extension == SupportedFileType.CSV.value:
            df.to_csv(full_local_file_path)
        
        elif extension == SupportedFileType.JSON.value:
            df.to_json(full_local_file_path)
        
        elif extension in [SupportedFileType.XLS.value, SupportedFileType.XLSX.value]:
            df.to_excel(full_local_file_path)

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

    def save_data(self, data_map):
        for output_filename, data_frame in data_map.items():
            logger.info(f'Writing {output_filename}')
            self._write_file(output_filename, data_frame)
