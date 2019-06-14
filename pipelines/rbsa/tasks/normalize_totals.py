import logging
import pandas as pd
from generics import task as t


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class NormalizeTotals(t.Task):
    """ 
    This class is used to group sites into 3 digit zip codes
    """
    def __init__(self, name):
        super().__init__(self)
        self.name = name
        self.input_artifact_total_loads = 'total_loads.csv'
        self.output_artifact_normal_loads = 'normal_loads.csv'
        self.my_data_files = [self.input_artifact_total_loads]
        self.task_function = self._task

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()
        
        self.df = data_map[self.input_artifact_total_loads]

         # output dataframe initialization
        initialization = True

        zipcodes = self.df.zipcode.unique()

        for zipcode in zipcodes:

            zipcode_df = self.df.loc[self.df.zipcode == zipcode]

            # get enduse columns
            print(zipcode_df.columns)

            # Normalize by peak total
            zipcode_df = zipcode_df/electric_percentage[enduse]
        
            # output dataframe 
            if initialization:
                normal_loads = zipcode_df
                initialization = False
            else:
                normal_loads = normal_loads.append(zipcode_df)

        self.validate(normal_loads)
        self.save_data({self.output_artifact_total_loads: normal_loads})


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
