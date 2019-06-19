import logging
import pandas as pd
from generics import task as t
from generics.file_type_enum import SupportedFileReadType


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class NormalizeLoadshapes(t.Task):
    """ 
    This class is used to group sites into 3 digit zip codes
    """
    def __init__(self, name):
        super().__init__(self)
        self.name = name
        self.input_artifact_enduse_loadshapes = 'enduse_loadshapes.csv'
        self.output_artifact_total_loadshapes = 'total_loadshapes.csv'
        self.my_data_files = [{ 'name': self.input_artifact_enduse_loadshapes, 'read_type': SupportedFileReadType.DATA }]
        self.task_function = self._task

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()
        
        self.df = data_map[self.input_artifact_enduse_loadshapes]

        if self.df.columns[0] == 'Unnamed: 0':
             self.df = self.df.drop('Unnamed: 0', axis=1)

         # output dataframe initialization
        initialization = True

        city_list = self.df.target.unique()

        for city in city_list:

            city_df = self.df.loc[self.df.target == city]
            city_df = city_df.set_index('time')
            city_df.index = pd.to_datetime(city_df.index)

            self.enduse_cols = list(city_df.columns)
            self.enduse_cols.remove('target')

            # get enduse columns
            normalization_val = self.get_normalization_val(city_df, summer=True)

            # Normalize by peak total
            city_df[self.enduse_cols] = city_df[self.enduse_cols]/normalization_val

            # output dataframe 
            if initialization:
                total_loadshapes = city_df
                initialization = False
            else:
                total_loadshapes = total_loadshapes.append(city_df)

        self.validate(total_loadshapes)
        self.on_complete({self.output_artifact_total_loadshapes: total_loadshapes})

    def get_normalization_val(self, city_df, summer):
        """
        returns peak total, if summer is True returns peak summer 
        """        

        if summer:
            city_df = city_df.loc[(city_df.index.month>5) & (city_df.index.month<9)]

        totals = city_df[self.enduse_cols].sum(axis=1)
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

    def on_failure(self):
        logger.info('Perform task cleanup because we failed')
        super().on_failure()
