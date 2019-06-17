import logging
import pandas as pd
from generics import task as t
from generics.file_type_enum import SupportedFileReadType


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class ZipcodeCorrelation(t.Task):
    def __init__(self, name):
        super().__init__(self)
        self.name = name

        self.input_artifact_full_zipcodes = 'full_zipcodes.csv'
        self.input_artifact_projection_locations = 'PROJECTION_LOCATIONS.json'

        # these will be used to generate list of input files
        self.pre_data_files = [ 
            { 'name': self.input_artifact_full_zipcodes, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_projection_locations, 'read_type': SupportedFileReadType.CONFIG }
        ]

        self.pre_data_map = self.load_data(self.pre_data_files) 
        self.full_zipcodes = list(self.pre_data_map['full_zipcodes.csv']['zipcodes'])  
        self.projection_locations = list(self.pre_data_map['PROJECTION_LOCATIONS.json']['cities'].keys())

        self.data_files = []

        for location in self.full_zipcodes:
            self.data_files.append({ 'name': 'tmy_base/'+str(location)+'.csv', 'read_type': SupportedFileReadType.DATA })

        for location in self.projection_locations:
            self.data_files.append({ 'name': 'tmy_target/'+str(location)+'.csv', 'read_type': SupportedFileReadType.DATA })

        self.task_function = self._task
        self.output_artifact_correlation_matrix = 'correlation_matrix.csv'
        
        self.data_map = None
        self.df = None

    def _save_data(self):
        self.save_data(self.df)

    def _get_data(self):
        self.data_map = self.load_data(self.data_files)       

    def _task(self):
        self._get_data()
        logger.info(self.df)

        self.validate()
        self.save_data({self.output_artifact_correlation_matrix: correlation_matrix})

    def validate(self, df):
        """
        Validation
        """
        logger.info(f'Validating task {self.name}')
        if df.isnull().values.any():
            logger.exception(f'Task {self.name} did not pass validation. Error found during grouping of sites to zip codes.')
            self.did_task_pass_validation = False
            self.on_failure()

    def on_failure(self):
        logger.info('Perform task cleanup because we failed')
        super().on_failure()
