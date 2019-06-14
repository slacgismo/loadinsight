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

            if zipcode_df.columns[0] == 'Unnamed: 0':
                zipcode_df = zipcode_df.drop('Unnamed: 0', axis=1)

            self.enduse_cols = list(zipcode_df.columns)
            self.enduse_cols.remove('time')
            self.enduse_cols.remove('zipcode')

            # get enduse columns
            normalization_val = self.get_normalization_val(zipcode_df)

            # Normalize by peak total
            zipcode_df[self.enduse_cols] = zipcode_df[self.enduse_cols]/normalization_val
        
            # output dataframe 
            if initialization:
                normal_loads = zipcode_df
                initialization = False
            else:
                normal_loads = normal_loads.append(zipcode_df)

        self.validate(normal_loads)
        self.save_data({self.output_artifact_normal_loads: normal_loads})


    def get_normalization_val(self, zipcode_df):
        """
        returns peak total
        """        

        totals = zipcode_df[self.enduse_cols].sum(axis=1)
        totals.index = zipcode_df['time']
        normalization_val = totals.max() # can be adjusted if normalizing for summer peak

        # can check here for totals.idxmax() month to confirm summer/winter

        return normalization_val

    def validate(self, df):
        """
        Validation
        """
        logger.info(f'Validating task {self.name}')

        if df.isnull().values.any():
            logger.exception(f'Task {self.name} did not pass validation. Error found during grouping of sites to zip codes.')
            self.did_task_pass_validation = False
            self.on_failure()

        totals = df[self.enduse_cols].sum(axis=1)

        if totals.max() > 1:
            logger.exception(f'Task {self.name} did not pass validation. Error found during grouping of sites to zip codes.')
            self.did_task_pass_validation = False
            self.on_failure()
            
    def on_failure(self):
        logger.info('Perform task cleanup because we failed')
        super().on_failure()
