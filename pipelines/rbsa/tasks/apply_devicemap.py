import logging
import pandas as pd
from generics import task as t
from generics.file_type_enum import SupportedFileReadType
import numpy as np

logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class ApplyDevicemap(t.Task):
    """ 
    This class is used to group sites into 3 digit zip codes
    """
    def __init__(self, name, pipeline_artifact_dir):
        super().__init__(self)
        self.name = name
        self.input_artifact_device_map = f'{pipeline_artifact_dir}/device_map.csv'
        self.input_artifact_y1 = f'{pipeline_artifact_dir}/combined_Y1.csv'
        self.input_artifact_y2 = f'{pipeline_artifact_dir}/combined_Y2.csv'
        self.output_artifact_clean_data = f'{pipeline_artifact_dir}/rbsa_cleandata.csv'
        self.my_data_files = [
            { 'name': self.input_artifact_device_map, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_y1, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_y2, 'read_type': SupportedFileReadType.DATA }
        ] 
        self.task_function = self._task

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()
        
        self.device_map = data_map[self.input_artifact_device_map]
   
        

        self.validate(rbsa_cleandata)
        self.on_complete({self.output_artifact_clean_data: rbsa_cleandata})           

    def validate(self, df):
        """
        Validation
        """
        logger.info(f'Validating task {self.name}')

        if df.isnull().values.any():
            logger.exception(f'Task {self.name} did not pass validation. DataFrame contains null values when it should not.')
            self.did_task_pass_validation = False
            self.on_failure()

    def on_failure(self):
        logger.info('Perform task cleanup because we failed')
        super().on_failure()
