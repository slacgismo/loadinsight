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
        self.input_artifact_normal_loads = 'ceus_normal_loads.csv'
        self.input_artifact_sensitivity_temperatures = 'SENSITIVITY_TEMPERATURES.json'
        self.output_artifact_loadshapes = 'ceus_loadshapes.csv'
        self.my_data_files = [
            { 'name': self.input_artifact_normal_loads, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_sensitivity_temperatures, 'read_type': SupportedFileReadType.CONFIG },
            { 'name': 'ceus_noaa/FCZ01.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'ceus_noaa/FCZ02.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'ceus_noaa/FCZ03.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'ceus_noaa/FCZ04.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'ceus_noaa/FCZ05.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'ceus_noaa/FCZ06.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'ceus_noaa/FCZ07.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'ceus_noaa/FCZ08.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'ceus_noaa/FCZ09.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'ceus_noaa/FCZ10.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'ceus_noaa/FCZ13.csv', 'read_type': SupportedFileReadType.DATA },
        ]
        self.task_function = self._task

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()
        
        self.sensitivity_temperatures = data_map[self.input_artifact_sensitivity_temperatures]['commercial']

        self.theat = self.sensitivity_temperatures['theat']
        self.tcool = self.sensitivity_temperatures['tcool']

        self.df = data_map[self.input_artifact_normal_loads]
        self.df = self.df.set_index(['time'])
        self.df.index = pd.to_datetime(self.df.index)

        if self.df.columns[0] == 'Unnamed: 0':
            self.df = self.df.drop('Unnamed: 0', axis=1)

        loadshapes = pd.DataFrame(columns=self.df.columns)

        climate_zones = self.df.fcz.unique()

        loadshapes = pd.DataFrame(columns=list(self.df.columns))

        for fcz in climate_zones:
            logger.info(f'calculating {fcz}')
            weather_file = f'ceus_noaa/{str(fcz)}.csv'
            weather = data_map[weather_file]
            weather = weather.set_index(['DATE'])
            weather.index = pd.to_datetime(weather.index)

            fcz_df = self.df.loc[self.df.fcz == fcz]

            self.enduse_cols = list(fcz_df.columns)
            self.enduse_cols.remove('fcz')
            self.enduse_cols.remove('buildingtype')

            buildingtypes = fcz_df.buildingtype.unique()

            for buildingtype in buildingtypes:

                buildingtype_df = fcz_df.loc[fcz_df.buildingtype == buildingtype]

                if weather.shape[0] > buildingtype_df.shape[0]:
                    weather = weather.loc[weather.index.isin(buildingtype_df.index)]

                A = self.get_A(weather)

                fcz_buildingtype_loadshapes = pd.DataFrame(columns=self.enduse_cols)

                for enduse in self.enduse_cols:
                    enduse_df = buildingtype_df[enduse]

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
                        x = self.get_baseload(enduse_df, A_base)
                        x = np.append(x, [0])
                        x = np.append(x, [0])

                    heat_sens = x[-2]
                    cool_sens = x[-1]

                    # adjust x by first value
                    x = np.append(x[0], x[1:48] + x[0])

                    if x[:48].min() < 0:
                        x[:48] -= x[:48].min()

                    x = np.append(x, [heat_sens, cool_sens])

                    fcz_buildingtype_loadshapes[enduse] = x 

                fcz_buildingtype_loadshapes.insert(loc=0, column='fcz', value=fcz)
                fcz_buildingtype_loadshapes.insert(loc=1, column='buildingtype', value=buildingtype)
                loadshapes = loadshapes.append(fcz_buildingtype_loadshapes)

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
