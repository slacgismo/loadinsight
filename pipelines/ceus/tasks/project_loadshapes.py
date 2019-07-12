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
    def __init__(self, name, pipeline_artifact_dir):
        super().__init__(self)
        self.name = name
        self.pipeline_artifact_dir = pipeline_artifact_dir
        self.input_artifact_loadshapes = f'{pipeline_artifact_dir}/ceus_loadshapes.csv'
        self.input_artifact_correlation_matrix = f'{pipeline_artifact_dir}/ceus_correlation_matrix.csv'
        self.input_artifact_sensitivity_temperatures = 'SENSITIVITY_TEMPERATURES.json'

        self.my_data_files = [
            { 'name': self.input_artifact_loadshapes, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_correlation_matrix, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_sensitivity_temperatures, 'read_type': SupportedFileReadType.CONFIG }
        ] 

        self.output_artifact_total_loadshapes = f'{pipeline_artifact_dir}/ceus_total_loadshapes.csv'
        self.task_function = self._task

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()

        self.sensitivity_temperatures = data_map[self.input_artifact_sensitivity_temperatures]['commercial']

        self.theat = self.sensitivity_temperatures['theat']
        self.tcool = self.sensitivity_temperatures['tcool']

        self.loadshapes = data_map[self.input_artifact_loadshapes]
        self.correlation_matrix = data_map[self.input_artifact_correlation_matrix]

        # organize df's
        if self.loadshapes.columns[0] == 'Unnamed: 0':
            self.loadshapes = self.loadshapes.drop('Unnamed: 0', axis=1)
        self.correlation_matrix = self.correlation_matrix.set_index(self.correlation_matrix.columns[0])
        
        self.enduse_cols = list(self.loadshapes.columns)
        self.enduse_cols.remove('fcz')
        self.enduse_cols.remove('buildingtype')

        buildingtypes = list(self.loadshapes.buildingtype.unique())

        total_loadshapes = pd.DataFrame(columns=['target', 'buildingtype', 'time', 'daytype'] + self.enduse_cols)

        target_locations = self.correlation_matrix.index 

        use_adjusted = False # can be made True to use adjusted weather

        for target in target_locations:
            base = self.correlation_matrix.loc[target,:].idxmax()

            try:
                weather_file = [{ 'name': f'{self.pipeline_artifact_dir}/target_weather/{str(target)}.csv', 'read_type': SupportedFileReadType.DATA }] 
                weather = self.load_data(weather_file)[f'{self.pipeline_artifact_dir}/target_weather/{str(target)}.csv']
            except:
                logger.warning(f'In task {self.name}, weather data for {target} not found')
                continue

            winter = weather['winter'] if not use_adjusted else weather['winter_adjusted']
            spring = weather['spring'] if not use_adjusted else weather['spring_adjusted']
            summer = weather['summer'] if not use_adjusted else weather['summer_adjusted']

            multiplier_winter = self.get_multiplier(winter)
            multiplier_spring = self.get_multiplier(spring)
            multiplier_summer = self.get_multiplier(summer)

            for buildingtype in buildingtypes:
                base_loadshapes = self.loadshapes.loc[(self.loadshapes['fcz'] == base) & (self.loadshapes['buildingtype'] == buildingtype)].copy()

                base_loadshapes["Ventilation"][:48] = base_loadshapes["Heating"][:48] + base_loadshapes["Cooling"][:48] + base_loadshapes["Ventilation"][:48]

                base_loadshapes["Heating"][:48] = 0
                base_loadshapes["Cooling"][:48] = 0

                target_loadshapes = pd.DataFrame(columns=self.enduse_cols)

                for enduse in self.enduse_cols:
                    x = np.array(base_loadshapes[enduse])

                    y_winter = []
                    y_spring = []
                    y_summer = []

                    for idx, val in enumerate(multiplier_winter):
                        if val < 0:
                            y_winter.append(x[idx] + (x[48] * val))
                        elif val > 0:
                            y_winter.append(x[idx] + (x[49] * val))
                        else:
                            y_winter.append(x[idx]) 

                    for idx, val in enumerate(multiplier_spring):
                        if val < 0:
                            y_spring.append(x[idx] + (x[48] * val))
                        elif val > 0:
                            y_spring.append(x[idx] + (x[49] * val))
                        else:
                            y_spring.append(x[idx])
             
                    for idx, val in enumerate(multiplier_summer):
                        if val < 0:
                            y_summer.append(x[idx] + (x[48] * val))
                        elif val > 0:
                            y_summer.append(x[idx] + (x[49] * val))
                        else:
                            y_summer.append(x[idx])

                    y = np.concatenate((y_winter, y_spring, y_summer), axis=0)

                    target_loadshapes[enduse] = y

                target_loadshapes.insert(loc=0, column='target', value=target)
                target_loadshapes.insert(loc=1, column='buildingtype', value=buildingtype)
                target_loadshapes.insert(loc=2, column='time', value=list(range(24)) * 3)
                target_loadshapes.insert(loc=3, column='daytype', value=(['winter_peak'] * 24) + (['spring_light'] * 24) + (['summer_peak'] * 24))

                total_loadshapes = total_loadshapes.append(target_loadshapes)

        self.validate(total_loadshapes)
        self.on_complete({self.output_artifact_total_loadshapes: total_loadshapes})

    def get_multiplier(self, weather):
        """multiplier array to get weather sensitive loadshapes 
        """
        multiplier_array = []

        for time_temp in weather:
            if self.theat <= time_temp <= self.tcool:
                multiplier_array.append(0)
            elif self.tcool < time_temp:
                multiplier_array.append(time_temp - self.tcool)
            else:
                multiplier_array.append(time_temp - self.theat)

        return multiplier_array

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
