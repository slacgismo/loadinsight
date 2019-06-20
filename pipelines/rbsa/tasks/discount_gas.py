import json
import logging
import pandas as pd
from generics import task as t
from generics.file_type_enum import SupportedFileReadType

logger = logging.getLogger('LCTK_APPLICATION_LOGGER')

class DiscountGas(t.Task):
    """ 
    This class is used to group sites into 3 digit zip codes
    """
    def __init__(self, name):
        super().__init__(self)
        self.name = name

        self.input_artifact_total_loadshapes = 'total_loadshapes.csv'
        self.input_artifact_gas_fraction = 'GAS_FRACTIONS.json'
        self.input_artifact_projection_locations = 'PROJECTION_LOCATIONS.json'
        self.my_data_files = [
            { 'name': self.input_artifact_total_loadshapes, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_gas_fraction, 'read_type': SupportedFileReadType.CONFIG },
            { 'name': self.input_artifact_projection_locations, 'read_type': SupportedFileReadType.CONFIG },
        ] 

        self.output_artifact_enduse_loadshapes = 'enduse_loadshapes.csv'
        self.task_function = self._task

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()

        self.df = data_map[self.input_artifact_total_loadshapes]
        self.gas_fraction = data_map[self.input_artifact_gas_fraction]
        self.projection_locations = data_map[self.input_artifact_projection_locations]


        if self.df.columns[0] == 'Unnamed: 0':
            self.df = self.df.drop('Unnamed: 0', axis=1)

        # output dataframe initialization
        initialization = True

        city_list = self.df.target.unique()

        for city in city_list:

            city_df = self.df.loc[self.df.target == city]

            zone = self.projection_locations['cities'][city]
            electric_percentage = self.gas_fraction['electrification'][zone]   

            # add gas fraction
            for enduse in electric_percentage.keys():
                city_df[enduse] = city_df[enduse] * city_df[enduse]
        
            # output dataframe 
            if initialization:
                enduse_loadshapes = city_df
                initialization = False
            else:
                enduse_loadshapes = enduse_loadshapes.append(city_df)

        self.validate(enduse_loadshapes)
        self.on_complete({self.output_artifact_enduse_loadshapes: enduse_loadshapes})

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
