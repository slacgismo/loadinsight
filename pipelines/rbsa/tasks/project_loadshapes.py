import json
import logging
import pandas as pd
import numpy as np
import datetime
from generics import task as t
from generics.file_type_enum import SupportedFileReadType

logger = logging.getLogger('LCTK_APPLICATION_LOGGER')

class ProjectLoadshapes(t.Task):
    """ 
    This class is used to group sites into 3 digit zip codes
    """
    def __init__(self, name):
        super().__init__(self)
        self.name = name

        self.input_artifact_loadshapes = 'loadshapes.csv'
        self.input_artifact_correlation_matrix = 'correlation_matrix.csv'

        self.my_data_files = [
            { 'name': self.input_artifact_loadshapes, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_correlation_matrix, 'read_type': SupportedFileReadType.DATA },
        ] 

        self.output_artifact_total_loadshapes = 'total_loadshapes.csv'
        self.task_function = self._task

        self.theat = 15
        self.tcool = 25

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()

        self.loadshapes = data_map[self.input_artifact_loadshapes]
        self.correlation_matrix = data_map[self.input_artifact_correlation_matrix]

        # organize df's
        if self.loadshapes.columns[0] == 'Unnamed: 0':
            self.loadshapes = self.loadshapes.drop('Unnamed: 0', axis=1)
        self.correlation_matrix = self.correlation_matrix.set_index(self.correlation_matrix.columns[0])
        
        self.enduse_cols = list(self.loadshapes.columns)
        self.enduse_cols.remove('zipcode')

        total_loadshapes = pd.DataFrame(columns=['target', 'time', 'daytype'] + self.enduse_cols)

        target_locations = self.correlation_matrix.index 

        use_adjusted = False # can be made True to use adjusted weather

        for target in target_locations:
            base = self.correlation_matrix.loc[target,:].idxmax()
            base_loadshapes = self.loadshapes.loc[self.loadshapes['zipcode'] == int(base)]

            try:
                weather_file = [{ 'name': f'target_weather/{str(target)}.csv', 'read_type': SupportedFileReadType.DATA }] 
                weather = self.load_data(weather_file)[f'target_weather/{str(target)}.csv']
            except:
                logger.warning(f'In task {self.name}, weather data for {target} not found')
                continue

            if use_adjusted:
                winter = weather['winter_adjusted']
                spring = weather['spring_adjusted']
                summer = weather['summer_Adjusted']
            else:
                winter = weather['winter']
                spring = weather['spring']
                summer = weather['summer']

            A_winter = self.get_A(winter)
            A_spring = self.get_A(spring)
            A_summer = self.get_A(summer)

            target_loadshapes = pd.DataFrame(columns=self.enduse_cols)

            for enduse in self.enduse_cols:

                x = np.array(base_loadshapes[enduse])

                y_winter = np.matmul(A_winter, x)
                y_spring = np.matmul(A_spring, x)
                y_summer = np.matmul(A_summer, x)

                y = np.concatenate((y_winter, y_spring, y_summer), axis=0)

                target_loadshapes[enduse] = y

            target_loadshapes.insert(loc=0, column='target', value=target)
            target_loadshapes.insert(loc=1, column='time', value=list(range(24))*3)
            target_loadshapes.insert(loc=2, column='daytype', value=(['winter_peak']*24) + (['spring_light']*24) + (['summer_peak']*24))

            total_loadshapes = total_loadshapes.append(target_loadshapes)

        self.validate(total_loadshapes)
        self.on_complete({self.output_artifact_total_loadshapes: total_loadshapes})

    def get_A(self, weather):
        """constructs A matrix for non weather sensitive loads
        """

        A = np.zeros((len(weather.index),50),float)

        for h in range(len(weather.index)):
            A[h][0] = 1
            hh = h%24
            A[h][hh] = 1.0

            if weather[h] < self.theat:
                A[h][(24*2)] = weather[h]-self.theat
            elif weather[h] > self.tcool:
                A[h][(24*2)+1] = weather[h]-self.tcool

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

    def on_failure(self):
        logger.info('Perform task cleanup because we failed')
        super().on_failure()
