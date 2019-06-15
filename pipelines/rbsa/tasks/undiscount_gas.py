import json
import logging
import pandas as pd
from generics import task as t
from generics.file_type_enum import SupportedFileReadType

logger = logging.getLogger('LCTK_APPLICATION_LOGGER')

class UndiscountGas(t.Task):
    """ 
    This class is used to group sites into 3 digit zip codes
    """
    def __init__(self, name):
        super().__init__(self)
        self.name = name

        self.input_artifact_enduse_loads = 'enduse_loads.csv'
        self.input_artifact_gas_fraction = 'GAS_FRACTIONS.json'
        self.input_artifact_zip_zone_map = 'ZIP_ZONE_MAP.json'
        self.my_data_files = [
            { 'name': self.input_artifact_enduse_loads, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_gas_fraction, 'read_type': SupportedFileReadType.CONFIG },
            { 'name': self.input_artifact_zip_zone_map, 'read_type': SupportedFileReadType.CONFIG },
        ] 

        self.output_artifact_total_loads = 'total_loads.csv'
        self.task_function = self._task

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()

        self.df = data_map[self.input_artifact_enduse_loads]
        self.gas_fraction = data_map[self.input_artifact_gas_fraction]
        self.zip_zone_map = data_map[self.input_artifact_zip_zone_map]

        # output dataframe initialization
        initialization = True

        zipcodes = self.df.zipcode.unique()

        for zipcode in zipcodes:

            zipcode_df = self.df.loc[self.df.zipcode == zipcode]

            zone = self.zip_zone_map['mapping'][str(zipcode)]
            electric_percentage = self.gas_fraction['electrification'][zone]   

            # add gas fraction
            for enduse in electric_percentage.keys():
                zipcode_df[enduse] = zipcode_df[enduse]/electric_percentage[enduse]
        
            # output dataframe 
            if initialization:
                total_loads = zipcode_df
                initialization = False
            else:
                total_loads = total_loads.append(zipcode_df)

        self.validate(total_loads)
        self.on_complete({self.output_artifact_total_loads: total_loads})

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
