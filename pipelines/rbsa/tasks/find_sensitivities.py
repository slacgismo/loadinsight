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
    def __init__(self, name, pipeline_artifact_dir):
        super().__init__(self)
        self.name = name
        self.pipeline_artifact_dir = pipeline_artifact_dir
        self.input_artifact_normal_loads = f'{pipeline_artifact_dir}/normal_loads.csv'
        self.input_artifact_sensitivity_temperatures = 'SENSITIVITY_TEMPERATURES.json'
        self.output_artifact_loadshapes = f'{pipeline_artifact_dir}/loadshapes.csv'
        self.my_data_files = [
            { 'name': self.input_artifact_normal_loads, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_sensitivity_temperatures, 'read_type': SupportedFileReadType.CONFIG },
            { 'name': f'{pipeline_artifact_dir}/noaa/594.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/596.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/597.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/598.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/833.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/835.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/836.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/837.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/838.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/970.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/971.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/972.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/973.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/974.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/980.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/981.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/982.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/983.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/984.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/985.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/988.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/989.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/990.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/991.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/992.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{pipeline_artifact_dir}/noaa/993.csv', 'read_type': SupportedFileReadType.DATA },
        ]
        self.task_function = self._task

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()
        
        self.sensitivity_temperatures = data_map[self.input_artifact_sensitivity_temperatures]['residential']

        self.theat = self.sensitivity_temperatures['theat']
        self.tcool = self.sensitivity_temperatures['tcool']

        self.df = data_map[self.input_artifact_normal_loads]
        self.df = self.df.set_index(['time'])
        self.df.index = pd.to_datetime(self.df.index)

        if self.df.columns[0] == 'Unnamed: 0':
            self.df = self.df.drop('Unnamed: 0', axis=1)

        loadshapes = pd.DataFrame(columns=self.df.columns)

        zipcodes = self.df.zipcode.unique()

        loadshapes = pd.DataFrame(columns=list(self.df.columns))

        for zipcode in zipcodes:
            weather_file = f'{self.pipeline_artifact_dir}/noaa/{str(zipcode)}.csv'
            weather = data_map[weather_file]
            weather = weather.set_index(['DATE'])
            weather.index = pd.to_datetime(weather.index)

            zipcode_df = self.df.loc[self.df.zipcode == zipcode]

            if weather.shape[0] > zipcode_df.shape[0]:
                weather = weather.loc[weather.index.isin(zipcode_df.index)]

            self.enduse_cols = list(zipcode_df.columns)
            self.enduse_cols.remove('zipcode')

            A = self.get_A(weather)

            zipcode_loadshapes = pd.DataFrame(columns=self.enduse_cols)

            for enduse in self.enduse_cols:
                enduse_df = zipcode_df[enduse]

                if enduse == 'Heating':
                    A_heat = A[:, :-1]
                    x = self.get_baseload(enduse_df, A_heat)
                    x = np.append(x, [0])

                elif enduse == 'Cooling':
                    A_cool = np.delete(A, -2, 1)
                    x = self.get_baseload(enduse_df, A_cool)
                    cooling_sensitivity = x[-1]
                    x = np.append(x[:-1], [0])
                    x = np.append(x, [cooling_sensitivity])

                else:
                    A_base = A[:, :-2]
                    if enduse == 'Ventilation':
                         x = self.get_baseload(enduse_df, A_base)
                    else:
                         x = self.get_baseload(enduse_df, A_base)
                    x = np.append(x, [0])
                    x = np.append(x, [0])

                heat_sens = x[-2]
                cool_sens = x[-1]

                if heat_sens > 0:
                    heat_sens = 0

                if cool_sens < 0:
                    cool_sens = 0

                # adjust x by first value
                x = np.append(x[0], x[1:48] + x[0])

                if x[:48].min() < 0:
                    logger.warning(f'In RBSA task {self.name}, {zipcode}-{enduse} baseload negative values have been cleaned.')
                    x[:48] -= x[:48].min()

                x = np.append(x, [heat_sens, cool_sens])

                zipcode_loadshapes[enduse] = x 

            zipcode_loadshapes.insert(loc=0, column='zipcode', value=zipcode)
            loadshapes = loadshapes.append(zipcode_loadshapes)

        self.validate(loadshapes)
        self.on_complete({self.output_artifact_loadshapes: loadshapes})

    def get_baseload(self, df, A):
        """ This function gets baseload for non weather sensitive enduses
        """
        At = A.transpose()
        y = np.asarray(df).transpose()
        M = np.matmul(np.linalg.inv(np.matmul(At, A)), At)
        x = np.matmul(M, y)

        return x

    def get_nonsensitive_A(self, weather):
        """
        Constructs A matrix for non weather sensitive loads
        """
        A = np.zeros((len(weather.index), 48), float)

        ts = datetime.datetime(weather.index[0].year, 1, 1, 0, 0, 0)
        dt = datetime.timedelta(hours=1) 

        for h in range(len(weather.index)):
            A[h][0] = 1
            hh = h % 24
            
            if weather.index[h].weekday() < 5:
                A[h][hh] = 1.0
            else:
                A[h][hh + 24] = 1.0

            ts += dt
        return A

    def get_A(self, weather):
        """
        Constructs A matrix for non weather sensitive loads
        """
        A = np.zeros((len(weather.index), 50), float)

        ts = datetime.datetime(weather.index[0].year, 1, 1, 0, 0, 0)
        dt = datetime.timedelta(hours=1) 

        for h in range(len(weather.index)):
            A[h][0] = 1
            hh = h % 24
            
            if weather.index[h].weekday() < 5:
                A[h][hh] = 1.0
            else:
                A[h][hh + 24] = 1.0

            if weather['Temperature'][h] < self.theat:
                A[h][(24 * 2)] = weather['Temperature'][h] - self.theat
            elif weather['Temperature'][h] > self.tcool:
                A[h][(24 * 2) + 1] = weather['Temperature'][h] - self.tcool

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
