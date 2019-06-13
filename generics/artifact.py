import os
import logging
import pandas as pd
from settings import base


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class ArtifactDataManager(object):
    """
    A Data Manager class responsible for reading and writing data from
    multiple sources and of various types. This class ought to be able 
    to read/write csv, xls and json files from the local FS and from the 
    a remote S3 bucket.
    """
    csv_extension = '.csv'
    json_extension = '.json'
    xls_extension = '.xls'
    xlsx_extension = '.xlsx'
    data_map = {}
    supported_file_types = [csv_extension, json_extension, xls_extension, xlsx_extension]

    def _parse_type(self, filename):
        name, extension = os.path.splitext(filename)
        extension = extension.lower()
        
        if extension not in self.supported_file_types:
            err_msg = (f'File {filename} with extension {extension} is not a supported type.'
                      f'Please choose a file with one the following types: {self.supported_file_types}')
            logger.exception(err_msg)
            raise TypeError(err_msg)

        return extension

    def _read_file(self, filename):
        extension = self._parse_type(filename)
        full_local_file_path = f'{base.LOCAL_PATH}/{filename}'
        
        try:            
            if extension == self.csv_extension:
                df = pd.read_csv(full_local_file_path)
            
            elif extension == self.json_extension:
                df = pd.read_json(full_local_file_path, typ='series')
            
            elif extension in [xls_extension, xlsx_extension]:
                df = pd.read_excel(full_local_file_path)
        
        except FileNotFoundError as fe:
            logger.exception(f'Could not find the file {filename} in the local system at local_data/')
            logger.info(f'Attempting to load {filename} from S3')
            raise fe
        
        return df

    def load_data(self, data_files):        
        for filename in data_files:
            logger.info(f'loading {filename}')            
            self.data_map[filename] = self._read_file(filename)

        return self.data_map

    def save_data(self, df):
        pass
