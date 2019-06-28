import logging
import pandas as pd
from generics import task as t
from generics.file_type_enum import SupportedFileReadType
import numpy as np

logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class GetMixed(t.Task):
    """ 
    This class is used to group sites into 3 digit zip codes
    """
    def __init__(self, name):
        super().__init__(self)
        self.name = name
        self.input_artifact_residential_mix = 'residential_mix.csv'
        self.input_artifact_commercial_mix = 'commercial_mix.csv'
        self.input_artifact_mixed_mix = 'mixed_mix.csv'
        self.input_artifact_rural_mix = 'rural_mix.csv'

        self.input_artifact_commercial_enduses = 'ceus_normal_loadshapes.csv'
        self.input_artifact_residential_enduse = 'normal_loadshapes.csv'

        self.input_artifact_commercial_components = 'ceus_components.csv'
        self.input_artifact_residential_components = 'components.csv'

        self.input_artifact_buildingtype_dict = 'BUILDINGTYPE_DICT.json'

        self.output_artifact_residential_mix = 'residential_mix_output.csv'
        self.output_artifact_commercial_mix = 'commercial_mix_output.csv'
        self.output_artifact_mixed_mix = 'mixed_mix_output.csv'
        self.output_artifact_rural_mix = 'rural_mix_output.csv'

        self.my_data_files = [
            { 'name': self.input_artifact_residential_mix, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_commercial_mix, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_mixed_mix, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_rural_mix, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_commercial_enduses, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_residential_enduse, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_commercial_components, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_residential_components, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_buildingtype_dict, 'read_type': SupportedFileReadType.CONFIG }
        ] 
        self.task_function = self._task

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()

        self.output_normalization_flag = True
        
        self.commercial_enduses = data_map[self.input_artifact_commercial_enduses]
        self.residential_enduses = data_map[self.input_artifact_residential_enduse]

        self.commercial_components = data_map[self.input_artifact_commercial_components]
        self.residential_components = data_map[self.input_artifact_residential_components]

        self.components = ['MotorA', 'MotorB', 'MotorC', 'MotorD', 'PE', 'Stat_P_Res', 'Stat_P_Cur']

        self.buildingtype_dict = data_map[self.input_artifact_buildingtype_dict]

        self.residential_mix = data_map[self.input_artifact_residential_mix]
        self.residential_mix = self.residential_mix.set_index(self.residential_mix.columns[0])

        residential_mix_output = self.get_mixed_output(self.residential_mix)
        residential_mix_output.insert(loc=1, column='mix', value='residential')

        self.commercial_mix = data_map[self.input_artifact_commercial_mix]
        self.commercial_mix = self.commercial_mix.set_index(self.commercial_mix.columns[0])

        commercial_mix_output = self.get_mixed_output(self.commercial_mix)
        commercial_mix_output.insert(loc=1, column='mix', value='commercial')

        self.mixed_mix = data_map[self.input_artifact_mixed_mix]
        self.mixed_mix = self.mixed_mix.set_index(self.mixed_mix.columns[0])

        mixed_mix_output = self.get_mixed_output(self.mixed_mix)
        mixed_mix_output.insert(loc=1, column='mix', value='mixed')

        self.rural_mix = data_map[self.input_artifact_rural_mix]
        self.rural_mix = self.rural_mix.set_index(self.rural_mix.columns[0])

        rural_mix_output = self.get_mixed_output(self.rural_mix)
        rural_mix_output.insert(loc=1, column='mix', value='rural')

        self.validate(residential_mix_output)
        self.validate(commercial_mix_output)
        self.validate(mixed_mix_output)
        self.validate(rural_mix_output)
        self.on_complete({self.output_artifact_residential_mix: residential_mix_output, self.output_artifact_commercial_mix: commercial_mix_output,
                        self.output_artifact_mixed_mix: mixed_mix_output, self.output_artifact_rural_mix: rural_mix_output})           

    def get_mixed_output(self, mix_chart): 
        """This function adapt the customer_chart
        """

        output_df = pd.DataFrame(columns=['target', 'daytype', 'time'] + self.components)
        
        for location in self.residential_components.target.unique():
            
            initialization = False

            for building in mix_chart.index:
                
                percent = mix_chart['Percent'][building]

                if building in self.buildingtype_dict.keys():
                    buildingtype = self.buildingtype_dict[building]

                    if buildingtype == 'RES':
                        df = self.residential_components.loc[self.residential_components.target == location]
                    else:
                        df = self.commercial_components.loc[(self.commercial_components.target == location) & (self.commercial_components.buildingtype == buildingtype)]
                    
                    df = df.reset_index() 

                else:
                    # if buildingtype doesn't exist
                    df = pd.DataFrame(1, index=range(72), columns=self.components)
                    df.insert(loc=0, column='target', value=location)
                    df.insert(loc=1, column='daytype', value= (['winter_peak'] * 24) + (['spring_light'] * 24) + (['summer_peak'] * 24))
                    df.insert(loc=2, column='time', value=list(range(24)) * 3)

                winter_peak = df[self.components][:24].sum(axis=1).max()
                spring_peak = df[self.components][24:48].sum(axis=1).max()
                summer_peak = df[self.components][48:72].sum(axis=1).max()

                total_peak = df[self.components].sum(axis=1).max()

                new_df = df[self.components].fillna(0)

                new_df[:24] = new_df[:24] * percent * (winter_peak / total_peak)
                new_df[24:48] = new_df[24:48] * percent * (spring_peak / total_peak)
                new_df[48:72] = new_df[48:72] * percent * (summer_peak / total_peak)

                if initialization:
                    mixed_df = mixed_df.add(new_df)
                else:
                    mixed_df = new_df
                    initialization = True

            if self.output_normalization_flag:
                mixed_df = self.normalize(mixed_df)
            
            mixed_df.insert(loc=0, column='target', value=location)
            mixed_df.insert(loc=1, column='daytype', value=df.daytype)
            mixed_df.insert(loc=2, column='time', value=df.time)
            output_df = output_df.append(mixed_df)

        return output_df

    def normalize(self, mixed_df):
        min_val = mixed_df.min().min()
        mixed_df = mixed_df - min_val

        max_val = mixed_df.sum(axis=1).max()
        mixed_df = mixed_df/max_val

        return mixed_df

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
