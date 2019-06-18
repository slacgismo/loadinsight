import logging
import pandas as pd
import numpy as np
import datetime
from generics import task as t
from generics.file_type_enum import SupportedFileReadType


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class FindSensitivities(t.Task):
    """ 
    This class is used to group sites into 3 digit zip codes
    """
    def __init__(self, name):
        super().__init__(self)
        self.name = name
        self.input_artifact_normal_loads = 'normal_loads.csv'
        self.output_artifact_loadshapes = 'loadshapes.csv'
        self.my_data_files = [
            { 'name': self.input_artifact_normal_loads, 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/594.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/596.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/597.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/598.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/833.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/835.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/836.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/837.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/838.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/970.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/971.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/972.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/973.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/974.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/980.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/981.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/982.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/983.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/984.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/985.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/988.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/989.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/990.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/991.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/992.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'noaa/993.csv', 'read_type': SupportedFileReadType.DATA },
        ]
        self.task_function = self._task

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()
        
        self.df = data_map[self.input_artifact_normal_loads]
        self.df = self.df.set_index(['time'])
        self.df.index = pd.to_datetime(self.df.index)

        if self.df.columns[0] == 'Unnamed: 0':
            self.df = self.df.drop('Unnamed: 0', axis=1)

        loadshapes = pd.DataFrame(columns=self.df.columns)

        zipcodes = self.df.zipcode.unique()

        loadshapes = pd.DataFrame(columns=list(self.df.columns))
        print(loadshapes.columns)

        for zipcode in zipcodes:

            weather_file = f'noaa/{str(zipcode)}.csv'
            weather = data_map[weather_file]
            weather = weather.set_index(['DATE'])
            weather.index = pd.to_datetime(weather.index)

            zipcode_df = self.df.loc[self.df.zipcode == zipcode]

            if weather.shape[0] > zipcode_df.shape[0]:
                weather = weather.reindex_like(zipcode_df).fillna(0)

            self.enduse_cols = list(zipcode_df.columns)
            self.enduse_cols.remove('zipcode')

            A_nonsensitive = self.get_nonsensitive_A(weather)

            zipcode_loadshapes = pd.DataFrame(columns=self.enduse_cols)

            for enduse in self.enduse_cols:
                enduse_df = zipcode_df[enduse]
                x = self.get_baseload(enduse_df, A_nonsensitive) # to create dummy output

                # if enduse == 'Heating':
                #     # get sensitivity
                #     continue
                # elif enduse == 'Cooling':
                #     continue
                # else:
                #     continue
                #     # x = self.get_baseload(enduse_df, A_nonsensitive)
                #     # print(enduse, x)

                zipcode_loadshapes[enduse] = x

            zipcode_loadshapes.insert(loc=0, column='zipcode', value=zipcode)

            # print(zipcode_loadshapes.head(4))


            loadshapes = loadshapes.append(zipcode_loadshapes)

        self.validate(loadshapes)
        self.on_complete({self.output_artifact_loadshapes: loadshapes})

    def get_baseload(self, df, A):
        """ This function gets baseload for non weather sensitive enduses
        """

        At = A.transpose()
        y = np.asarray(df).transpose()
        M = np.matmul(np.linalg.inv(np.matmul(At,A)),At)
        # print(M.min()) 
        x = np.matmul(M,y)

        return x

    def get_nonsensitive_A(self, weather):
        """constructs A matrix for non weather sensitive loads
        """

        A = np.zeros((len(weather.index),48),float)

        ts = datetime.datetime(weather.index[0].year,1,1,0,0,0)
        dt = datetime.timedelta(hours=1) 

        for h in range(len(weather.index)):
            A[h][0] = 1
            hh = h%24
            
            if weather.index[h].weekday() < 5:
                A[h][hh] = 1.0
            else:
                A[h][hh+24] = 1.0

            ts += dt

        return A

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
            
    def on_failure(self):
        logger.info('Perform task cleanup because we failed')
        super().on_failure()
