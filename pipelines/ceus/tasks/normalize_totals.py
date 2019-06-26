import logging
import pandas as pd
from generics import task as t
from generics.file_type_enum import SupportedFileReadType


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class NormalizeTotals(t.Task):
    """ 
    This class is used to group sites into 3 digit zip codes
    """
    def __init__(self, name):
        super().__init__(self)
        self.name = name
        self.input_artifact_total_loads = 'ceus_total_loads.csv'
        self.output_artifact_normal_loads = 'ceus_normal_loads.csv'
        self.my_data_files = [{ 'name': self.input_artifact_total_loads, 'read_type': SupportedFileReadType.DATA }]
        self.task_function = self._task

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()
        
        self.df = data_map[self.input_artifact_total_loads]
        
        normal_loads = pd.DataFrame()
        climate_zones = self.df.fcz.unique()

        for fcz in climate_zones:

            fcz_df = self.df.loc[self.df.fcz == fcz].copy()

            if fcz_df.columns[0] == 'Unnamed: 0':
                fcz_df = fcz_df.drop('Unnamed: 0', axis=1)

            self.enduse_cols = list(fcz_df.columns)
            self.enduse_cols.remove('time')
            self.enduse_cols.remove('fcz')
            self.enduse_cols.remove('buildingtype')

            buildingtypes = fcz_df.buildingtype.unique()

            for buildingtype in buildingtypes:
                buildingtype_df = fcz_df.loc[fcz_df.buildingtype == buildingtype].copy()

                min_val = buildingtype_df[self.enduse_cols].min().min()
                buildingtype_df[self.enduse_cols] = buildingtype_df[self.enduse_cols] - min_val
            
                # get enduse columns
                normalization_val = self.get_normalization_val(buildingtype_df)

                # Normalize by peak total
                buildingtype_df[self.enduse_cols] = buildingtype_df[self.enduse_cols]/normalization_val

                normal_loads = normal_loads.append(buildingtype_df)

        self.validate(normal_loads)
        self.on_complete({self.output_artifact_normal_loads: normal_loads})

    def get_normalization_val(self, buildingtype_df):
        """
        returns peak total
        """        

        totals = buildingtype_df[self.enduse_cols].sum(axis=1)
        totals.index = buildingtype_df['time']
        normalization_val = totals.max() # can be adjusted if normalizing for summer peak

        # can check here for totals.idxmax() month to confirm summer/winter

        return normalization_val

    def validate(self, df):
        """
        Validation
        """
        logger.info(f'Validating task {self.name}')

        if df.isnull().values.any():
            logger.exception(f'Task {self.name} did not pass validation. DataFrame contains null values when it should not.')
            self.did_task_pass_validation = False
            self.on_failure()

        totals = df[self.enduse_cols].sum(axis=1)

        if round(totals.max(), 3) > 1:
            logger.exception(f'Task {self.name} did not pass validation. Peak total greater than 1.')
            self.did_task_pass_validation = False
            self.on_failure()

        if df.min(numeric_only=True).min() < 0:
            logger.exception(f'Task {self.name} did not pass validation. Negative value found.')
            self.did_task_pass_validation = False
            self.on_failure()

            
    def on_failure(self):
        logger.info('Perform task cleanup because we failed')
        super().on_failure()
