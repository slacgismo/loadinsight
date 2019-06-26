import logging
import pandas as pd
import numpy as np
from generics import task as t
from generics.file_type_enum import SupportedFileReadType


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class FczCorrelation(t.Task):
    def __init__(self, name):
        super().__init__(self)
        self.name = name

        self.input_artifact_normal_loads = 'ceus_normal_loads.csv'
        self.input_artifact_projection_locations = 'PROJECTION_LOCATIONS.json'

        # these will be used to generate list of input files
        self.pre_data_files = [ 
            { 'name': self.input_artifact_normal_loads, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_projection_locations, 'read_type': SupportedFileReadType.CONFIG }
        ]

        self.task_function = self._task
        self.output_artifact_correlation_matrix = 'ceus_correlation_matrix.csv'
        
        self.data_map = None
        self.df = None

    def _get_data(self):
        self.pre_data_map = self.load_data(self.pre_data_files) 
        self.fcz_names = list(self.pre_data_map[self.input_artifact_normal_loads].fcz.unique())
        self.projection_locations = list(self.pre_data_map['PROJECTION_LOCATIONS.json']['cities'].keys())

        self.data_files = []

        for location in self.fcz_names:
            self.data_files.append({ 'name': f'ceus_tmy_base/{str(location)}.csv', 'read_type': SupportedFileReadType.DATA })

        for location in self.projection_locations:
            self.data_files.append({ 'name': f'ceus_tmy_target/{str(location)}.csv', 'read_type': SupportedFileReadType.DATA })

        self.data_map = self.load_data(self.data_files)       

    def _task(self):
        self._get_data()
        logger.info(self.df)

        correlation_metrics = ['Temperature', 'Solar Zenith Angle', 'GHI', 'DHI', 'DNI', 'Wind Speed', 'Wind Direction', 'Relative Humidity']    
        correlation_matrix = pd.DataFrame(0, index=self.projection_locations, columns=self.fcz_names)

        for base in self.fcz_names:
            for target in self.projection_locations:
                    
                base_filename = f'ceus_tmy_base/{str(base)}.csv'
                target_filename = f'ceus_tmy_target/{str(target)}.csv'

                base_weather = self.data_map[base_filename]
                target_weather = self.data_map[target_filename]

                base_weather = base_weather.set_index(base_weather.columns[0])
                target_weather = target_weather.set_index(target_weather.columns[0])

                if base_weather.shape != target_weather.shape:
                    logger.warning(f'Task {self.name} did not pass validation. TMY weather data sizes do not match for {base} and {target}.')
                    continue

                correlation_vals = []
                for metric in correlation_metrics:       
                    correlation_vals.append(np.ma.corrcoef(base_weather[metric], target_weather[metric])[0][1])

                correlation = sum(correlation_vals) / len(correlation_vals)
                correlation_matrix.ix[target, base] = correlation

        self.validate(correlation_matrix)
        self.on_complete({self.output_artifact_correlation_matrix: correlation_matrix})

    def validate(self, df):
        """
        Validation
        """
        logger.info(f'Validating task {self.name}')
        if df.isnull().values.any():
            logger.exception(f'Task {self.name} did not pass validation. Error found during generating correlation matrix.')
            self.did_task_pass_validation = False
            self.on_failure()

    def on_failure(self):
        logger.info('Perform task cleanup because we failed')
        super().on_failure()
