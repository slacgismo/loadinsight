import logging
import pandas as pd
from generics import task as t
import json

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
        self.my_data_files = [self.input_artifact_enduse_loads] 

        self.output_artifact_total_loads = 'total_loads.csv'
        
        self.task_function = self._task

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()

        self.df = data_map[self.input_artifact_enduse_loads]

        # TODO replace with json reader
        with open('config/GAS_FRACTIONS.json') as json_file:  
            self.gas_fraction = json.load(json_file) 

        with open('config/ZIP_ZONE_MAP.json') as json_file:  
            self.zip_zone_map = json.load(json_file) 

        # output dataframe initialization
        initialization = True

        zipcodes = self.df.zipcode.unique()

        for zipcode in zipcodes:

            print(zipcode)

            zipcode_df = self.df.loc[self.df.zipcode == zipcode]
            zipcode_df = zipcode_df.reset_index()

            zone = self.zip_zone_map['mapping'][str(zipcode)]
            electric_percentage = self.gas_fraction['electrification'][zone]

            print(electric_percentage)


        

        # self.validate(total_loads)
        # self.save_data({self.output_artifact_total_loads: total_loads})

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
