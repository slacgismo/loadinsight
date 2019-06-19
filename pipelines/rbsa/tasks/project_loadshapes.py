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

        total_loadshapes = pd.DataFrame(columns=['target', 'time'] + self.enduse_cols)

        target_locations = self.correlation_matrix.index 

        for target in target_locations:
            base = self.correlation_matrix.loc[target,:].idxmax()
            base_loadshapes = self.loadshapes.loc[self.loadshapes['zipcode'] == int(base)]

            # to be replaced with 90% weather
            weather_file = [{ 'name': f'tmy_target/{str(target)}.csv', 'read_type': SupportedFileReadType.DATA }] 
            weather = self.load_data(weather_file)[f'tmy_target/{str(target)}.csv']
            weather = weather.set_index(weather.columns[0])
            weather.index = pd.to_datetime(weather.index)
            weather = weather['Temperature']

            target_loadshapes = pd.DataFrame(columns=self.enduse_cols)

            A = self.get_A(weather)

            for enduse in self.enduse_cols:
                x = np.array(base_loadshapes[enduse])
                y = np.matmul(A, x)

                target_loadshapes[enduse] = y

            target_loadshapes.insert(loc=0, column='target', value=target)
            target_loadshapes.insert(loc=1, column='time', value=weather.index)

            total_loadshapes = total_loadshapes.append(target_loadshapes)

        self.validate(total_loadshapes)
        self.on_complete({self.output_artifact_total_loadshapes: total_loadshapes})

    def get_A(self, weather):
        """constructs A matrix for non weather sensitive loads
        """

        A = np.zeros((len(weather.index),50),float)

        ts = datetime.datetime(weather.index[0].year,1,1,0,0,0)
        dt = datetime.timedelta(hours=1) 

        for h in range(len(weather.index)):
            A[h][0] = 1
            hh = h%24
            
            if weather.index[h].weekday() < 5:
                A[h][hh] = 1.0
            else:
                A[h][hh+24] = 1.0

            if weather[h] < self.theat:
                A[h][(24*2)] = weather[h]-self.theat
            elif weather[h] > self.tcool:
                A[h][(24*2)+1] = weather[h]-self.tcool

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

    def on_failure(self):
        logger.info('Perform task cleanup because we failed')
        super().on_failure()
