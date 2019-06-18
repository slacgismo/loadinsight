import logging
import pandas as pd
import numpy as np
from generics import task as t
from generics.file_type_enum import SupportedFileReadType


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class ZipcodeCorrelation(t.Task):
    def __init__(self, name):
        super().__init__(self)
        self.name = name

        self.input_artifact_full_zipcodes = 'full_zipcodes.csv'
        self.input_artifact_projection_locations = 'PROJECTION_LOCATIONS.json'

        # these will be used to generate list of input files
        self.pre_data_files = [ 
            { 'name': self.input_artifact_full_zipcodes, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_projection_locations, 'read_type': SupportedFileReadType.CONFIG }
        ]

        self.task_function = self._task
        self.output_artifact_correlation_matrix = 'correlation_matrix.csv'
        
        self.data_map = None
        self.df = None

    def _save_data(self):
        self.save_data(self.df)

    def _get_data(self):
        self.pre_data_map = self.load_data(self.pre_data_files) 
        self.full_zipcodes = list(self.pre_data_map['full_zipcodes.csv']['zipcodes'])  
        self.projection_locations = list(self.pre_data_map['PROJECTION_LOCATIONS.json']['cities'].keys())

        self.data_files = []

        for location in self.full_zipcodes:
            self.data_files.append({ 'name': 'tmy_base/'+str(location)+'.csv', 'read_type': SupportedFileReadType.DATA })

        for location in self.projection_locations:
            self.data_files.append({ 'name': 'tmy_target/'+str(location)+'.csv', 'read_type': SupportedFileReadType.DATA })

        self.data_map = self.load_data(self.data_files)       

    def _task(self):
        self._get_data()
        logger.info(self.df)

        correlation_metrics = ['Temperature', 'Solar Zenith Angle', 'GHI', 'DHI', 'DNI', 'Wind Speed', 'Wind Direction', 'Relative Humidity']    
        correlation_matrix = pd.DataFrame(0, index=self.projection_locations, columns=self.full_zipcodes)

        for base in self.full_zipcodes:
            for target in self.projection_locations:

                base_filename = 'tmy_base/'+str(base)+'.csv'
                target_filename = 'tmy_target/'+str(target)+'.csv'

                base_weather = self.data_map[base_filename]
                target_weather = self.data_map[target_filename]

                base_weather = base_weather.set_index(base_weather.columns[0])
                target_weather = target_weather.set_index(target_weather.columns[0])

                if base_weather.shape != target_weather.shape:
                    logger.exception(f'Task {self.name} did not pass validation. TMY weather data sizes do not match for {base} and {target}.')
                    continue

                correlation_vals = []
                for metric in correlation_metrics:       
                    correlation_vals.append(np.ma.corrcoef(base_weather[metric], target_weather[metric])[0][1])

                correlation = sum(correlation_vals)/len(correlation_vals)
                correlation_matrix.ix[target,base] = correlation

        correlation_matrix = self.convert_coef_3digit(correlation_matrix)

        self.validate(correlation_matrix)
        self.on_complete({self.output_artifact_correlation_matrix: correlation_matrix})

    def convert_coef_3digit(self, df):
        """ Convert correlation of coefficient array from 5 digit zipcodes to 3
        """
        zipcodes_3digit = set()

        for zipcode in self.full_zipcodes:
            zipcodes_3digit.add(str(zipcode)[0:3])

        new_coef = pd.DataFrame(columns=zipcodes_3digit, index=self.projection_locations)

        for city in self.projection_locations:
            row = df.loc[df.index == city].squeeze()
            ordered_row = row.sort_values(ascending=False)

            for index in ordered_row.index:
                cell_val = new_coef.loc[city][str(index)[:3]]

                if cell_val != cell_val:
                    new_coef.at[city,str(index)[:3]] = ordered_row[index]

        new_coef = new_coef.reindex(sorted(new_coef.columns), axis=1)

        return new_coef

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
