import logging
import numpy as np
import pandas as pd
from generics import task as t
from generics.file_type_enum import SupportedFileReadType

logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class GetMixed(t.Task):
    """ 
    This class is used to group sites into 3 digit zip codes
    """
    def __init__(self, name, pipeline_artifact_dir):
        super().__init__(self)
        self.name = name
        self.input_artifact_residential_mix = f'{pipeline_artifact_dir}/residential_mix.csv'
        self.input_artifact_commercial_mix = f'{pipeline_artifact_dir}/commercial_mix.csv'
        self.input_artifact_mixed_mix = f'{pipeline_artifact_dir}/mixed_mix.csv'
        self.input_artifact_rural_mix = f'{pipeline_artifact_dir}/rural_mix.csv'

        self.input_artifact_residential_components = 'rbsa/components.csv'
        self.input_artifact_residential_enduse = 'rbsa/normal_loadshapes.csv'
        self.input_artifact_commercial_components = 'ceus/ceus_components.csv'
        self.input_artifact_commercial_enduses = 'ceus/ceus_normal_loadshapes.csv'

        self.input_artifact_buildingtype_dict = 'BUILDINGTYPE_DICT.json'

        self.output_artifact_residential_mix = f'{pipeline_artifact_dir}/suburban_mix_output.csv'
        self.output_artifact_commercial_mix = f'{pipeline_artifact_dir}/urban_mix_output.csv'
        self.output_artifact_mixed_mix = f'{pipeline_artifact_dir}/mixed_mix_output.csv'
        self.output_artifact_rural_mix = f'{pipeline_artifact_dir}/rural_mix_output.csv'

        self.output_artifact_residential_mix_hour_norm = f'{pipeline_artifact_dir}/suburban_mix_output_hour_norm.csv'
        self.output_artifact_commercial_mix_hour_norm = f'{pipeline_artifact_dir}/urban_mix_output_hour_norm.csv'
        self.output_artifact_mixed_mix_hour_norm = f'{pipeline_artifact_dir}/mixed_mix_output_hour_norm.csv'
        self.output_artifact_rural_mix_hour_norm = f'{pipeline_artifact_dir}/rural_mix_output_hour_norm.csv'

        self.output_artifact_residential_mix_study_hours = f'{pipeline_artifact_dir}/suburban_mix_output_study_hours.csv'
        self.output_artifact_commercial_mix_study_hours = f'{pipeline_artifact_dir}/urban_mix_output_study_hours.csv'
        self.output_artifact_mixed_mix_study_hours = f'{pipeline_artifact_dir}/mixed_mix_output_study_hours.csv'
        self.output_artifact_rural_mix_study_hours = f'{pipeline_artifact_dir}/rural_mix_output_study_hours.csv'

        self.output_artifact_residential_mix_study_hours_hour_norm = f'{pipeline_artifact_dir}/suburban_mix_output_study_hours_hour_norm.csv'
        self.output_artifact_commercial_mix_study_hours_hour_norm = f'{pipeline_artifact_dir}/urban_mix_output_study_hours_hour_norm.csv'
        self.output_artifact_mixed_mix_study_hours_hour_norm = f'{pipeline_artifact_dir}/mixed_mix_output_study_hours_hour_norm.csv'
        self.output_artifact_rural_mix_study_hours_hour_norm = f'{pipeline_artifact_dir}/rural_mix_output_study_hours_hour_norm.csv'

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
        self.summer_normalization = True

        self.study_periods = {
            'winter_peak' : [8],
            'spring_light' : [3],
            'summer_peak' : [16]
        }
        
        self.commercial_enduses = data_map[self.input_artifact_commercial_enduses]
        self.residential_enduses = data_map[self.input_artifact_residential_enduse]

        self.commercial_components = data_map[self.input_artifact_commercial_components]
        self.residential_components = data_map[self.input_artifact_residential_components]

        self.components = ['MotorA', 'MotorB', 'MotorC', 'MotorD', 'PE', 'Stat_P_Res', 'Stat_P_Cur']

        self.buildingtype_dict = data_map[self.input_artifact_buildingtype_dict]

        self.residential_mix = data_map[self.input_artifact_residential_mix]
        self.residential_mix = self.residential_mix.set_index(self.residential_mix.columns[0])

        residential_mix_output, residential_mix_output_hour_norm = self.get_mixed_output(self.residential_mix)
        residential_mix_output.insert(loc=1, column='mix', value='residential')
        residential_mix_output_hour_norm.insert(loc=1, column='mix', value='residential')
        residential_mix_study_hours = self.get_study_period(residential_mix_output)
        residential_mix_study_hours_hour_norm = self.get_study_period(residential_mix_output_hour_norm)

        self.commercial_mix = data_map[self.input_artifact_commercial_mix]
        self.commercial_mix = self.commercial_mix.set_index(self.commercial_mix.columns[0])

        commercial_mix_output, commercial_mix_output_hour_norm = self.get_mixed_output(self.commercial_mix)
        commercial_mix_output.insert(loc=1, column='mix', value='commercial')
        commercial_mix_output_hour_norm.insert(loc=1, column='mix', value='commercial')
        commercial_mix_study_hours = self.get_study_period(commercial_mix_output)
        commercial_mix_study_hours_hour_norm = self.get_study_period(commercial_mix_output_hour_norm)

        self.mixed_mix = data_map[self.input_artifact_mixed_mix]
        self.mixed_mix = self.mixed_mix.set_index(self.mixed_mix.columns[0])

        mixed_mix_output, mixed_mix_output_hour_norm = self.get_mixed_output(self.mixed_mix)
        mixed_mix_output.insert(loc=1, column='mix', value='mixed')
        mixed_mix_output_hour_norm.insert(loc=1, column='mix', value='mixed')
        mixed_mix_study_hours = self.get_study_period(mixed_mix_output)
        mixed_mix_study_hours_hour_norm = self.get_study_period(mixed_mix_output_hour_norm)

        self.rural_mix = data_map[self.input_artifact_rural_mix]
        self.rural_mix = self.rural_mix.set_index(self.rural_mix.columns[0])

        rural_mix_output, rural_mix_output_hour_norm = self.get_mixed_output(self.rural_mix)
        rural_mix_output.insert(loc=1, column='mix', value='rural')
        rural_mix_output_hour_norm.insert(loc=1, column='mix', value='rural')
        rural_mix_study_hours = self.get_study_period(rural_mix_output)
        rural_mix_study_hours_hour_norm = self.get_study_period(rural_mix_output_hour_norm)

        self.validate(residential_mix_output)
        self.validate(commercial_mix_output)
        self.validate(mixed_mix_output)
        self.validate(rural_mix_output)
        self.on_complete({
            self.output_artifact_residential_mix: residential_mix_output,
            self.output_artifact_commercial_mix: commercial_mix_output,
            self.output_artifact_mixed_mix: mixed_mix_output,
            self.output_artifact_rural_mix: rural_mix_output,            
            self.output_artifact_residential_mix_hour_norm: residential_mix_output_hour_norm,
            self.output_artifact_commercial_mix_hour_norm: commercial_mix_output_hour_norm,
            self.output_artifact_mixed_mix_hour_norm: mixed_mix_output_hour_norm,
            self.output_artifact_rural_mix_hour_norm: rural_mix_output_hour_norm,            
            self.output_artifact_residential_mix_study_hours: residential_mix_study_hours,
            self.output_artifact_commercial_mix_study_hours: commercial_mix_study_hours,
            self.output_artifact_mixed_mix_study_hours: mixed_mix_study_hours,
            self.output_artifact_rural_mix_study_hours: rural_mix_study_hours,            
            self.output_artifact_residential_mix_study_hours_hour_norm: residential_mix_study_hours_hour_norm,
            self.output_artifact_commercial_mix_study_hours_hour_norm: commercial_mix_study_hours_hour_norm,
            self.output_artifact_mixed_mix_study_hours_hour_norm: mixed_mix_study_hours_hour_norm,
            self.output_artifact_rural_mix_study_hours_hour_norm: rural_mix_study_hours_hour_norm
        })

    def get_mixed_output(self, mix_chart): 
        """
        This function adapt the customer_chart
        """
        output_df = pd.DataFrame(columns=['target', 'daytype', 'time'] + self.components)
        output_df_hourly_normalized = pd.DataFrame(columns=['target', 'daytype', 'time'] + self.components)
        
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
                    if building == 'Pumping':
                        df = pd.DataFrame(0, index=range(72), columns=self.components)
                        df['MotorC'] = 1
                        df.insert(loc=0, column='target', value=location)
                        df.insert(loc=1, column='daytype', value= (['winter_peak'] * 24) + (['spring_light'] * 24) + (['summer_peak'] * 24))
                        df.insert(loc=2, column='time', value=list(range(24)) * 3)    
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
                mixed_df = self.normalize(mixed_df, self.summer_normalization)

            mixed_hourly_normalized = self.normalize_hourly(mixed_df)
            
            mixed_df.insert(loc=0, column='target', value=location)
            mixed_df.insert(loc=1, column='daytype', value=df.daytype)
            mixed_df.insert(loc=2, column='time', value=df.time)

            mixed_hourly_normalized.insert(loc=0, column='target', value=location)
            mixed_hourly_normalized.insert(loc=1, column='daytype', value=df.daytype)
            mixed_hourly_normalized.insert(loc=2, column='time', value=df.time)

            output_df = output_df.append(mixed_df)
            output_df_hourly_normalized = output_df_hourly_normalized.append(mixed_hourly_normalized)

        return output_df, output_df_hourly_normalized

    def normalize(self, mixed_df, summer_normalization):
        """
        if summer_normalization == True it will be normalized to peak summer
        """
        min_val = mixed_df.min().min()
        mixed_df = mixed_df - min_val

        if summer_normalization:
            max_val = mixed_df[48:72].sum(axis=1).max()
        else:
            max_val = mixed_df.sum(axis=1).max()

        mixed_df = mixed_df/max_val

        return mixed_df

    def normalize_hourly(self, mixed_df):
        mixed_hourly_normalzied = mixed_df.div(mixed_df.sum(axis=1), axis=0)

        return mixed_hourly_normalzied

    def get_study_period(self, full_df):
        daytypes = self.study_periods.keys()
        study_hours_df = pd.DataFrame(columns=full_df.columns)

        for daytype in daytypes:
            for time in self.study_periods[daytype]:
                day_df = full_df.loc[(full_df['daytype'] == daytype) & (full_df['time'] == time)]
                study_hours_df = study_hours_df.append(day_df)

        return study_hours_df

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
