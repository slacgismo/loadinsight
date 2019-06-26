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

        self.input_artifact_ceus_cleandata = 'ceus_cleandata.csv'
        self.input_artifact_gas_fraction = 'GAS_FRACTIONS.json'
        self.input_artifact_zip_zone_map = 'ZIP_ZONE_MAP.json'
        self.my_data_files = [
            { 'name': self.input_artifact_ceus_cleandata, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_gas_fraction, 'read_type': SupportedFileReadType.CONFIG },
            { 'name': self.input_artifact_zip_zone_map, 'read_type': SupportedFileReadType.CONFIG },
        ] 

        self.output_artifact_total_loads = 'ceus_total_loads.csv'
        self.task_function = self._task

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()

        self.df = data_map[self.input_artifact_ceus_cleandata]
        self.gas_fraction = data_map[self.input_artifact_gas_fraction]
        self.zip_zone_map = data_map[self.input_artifact_zip_zone_map]

        total_loads = pd.DataFrame()
        climate_zones = self.df.fcz.unique()

        for fcz in climate_zones:

            print(fcz)

            fcz_df = self.df.loc[self.df.fcz == fcz].copy()
            zone = self.zip_zone_map['mapping'][str(fcz)]
            electric_percentage = self.gas_fraction['electrification'][zone]   

            print(fcz_df)

            print(electric_percentage)

            # add gas fraction
            for enduse in electric_percentage.keys():
                fcz_df[enduse] = fcz_df[enduse] / electric_percentage[enduse]
            
            total_loads = total_loads.append(fcz_df)

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
        
        if df.min(numeric_only=True).min() < 0:
            logger.exception(f'Task {self.name} did not pass validation. Negative value found.')
            self.did_task_pass_validation = False
            self.on_failure()

    def on_failure(self):
        logger.info('Perform task cleanup because we failed')
        super().on_failure()
