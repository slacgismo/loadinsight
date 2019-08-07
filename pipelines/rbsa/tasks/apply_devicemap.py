import logging
import pandas as pd
from generics import task as t
from generics.file_type_enum import SupportedFileReadType
import numpy as np

logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class ApplyDevicemap(t.Task):
    """ 
    This class is used to group sites into 3 digit zip codes
    """
    def __init__(self, name, pipeline_artifact_dir):
        super().__init__(self)
        self.name = name
        self.input_artifact_device_map = f'{pipeline_artifact_dir}/device_map.csv'
        self.input_artifact_excluded_locations = 'EXCLUDED_LOCATIONS.json'

        self.input_artifact_y1_1 = f'{pipeline_artifact_dir}/raw/RBSAM_Y1_PART 1 OF 4.csv'
        self.input_artifact_y1_2 = f'{pipeline_artifact_dir}/raw/RBSAM_Y1_PART 2 OF 4.csv'
        self.input_artifact_y1_3 = f'{pipeline_artifact_dir}/raw/RBSAM_Y1_PART 3 OF 4.csv'
        self.input_artifact_y1_4 = f'{pipeline_artifact_dir}/raw/RBSAM_Y1_PART 4 OF 4.csv'
        self.input_artifact_y2_1 = f'{pipeline_artifact_dir}/raw/RBSAM_Y2_PART 1 OF 5.csv'
        self.input_artifact_y2_2 = f'{pipeline_artifact_dir}/raw/RBSAM_Y2_PART 2 OF 5.csv'
        self.input_artifact_y2_3 = f'{pipeline_artifact_dir}/raw/RBSAM_Y2_PART 3 OF 5.csv'
        self.input_artifact_y2_4 = f'{pipeline_artifact_dir}/raw/RBSAM_Y2_PART 4 OF 5.csv'
        self.input_artifact_y2_5 = f'{pipeline_artifact_dir}/raw/RBSAM_Y2_PART 5 OF 5.csv'

        self.Y1_files = [self.input_artifact_y1_1, self.input_artifact_y1_2, self.input_artifact_y1_3, self.input_artifact_y1_4]
        self.Y2_files = [
            self.input_artifact_y2_1, self.input_artifact_y2_2, self.input_artifact_y2_3, self.input_artifact_y2_4, 
            self.input_artifact_y2_5
        ]

        self.output_artifact_clean_data = f'{pipeline_artifact_dir}/rbsa_cleandata.csv'
        self.my_data_files = [
            { 'name': self.input_artifact_device_map, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_excluded_locations, 'read_type': SupportedFileReadType.CONFIG },
            { 'name': self.input_artifact_y1_1, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_y1_2, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_y1_3, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_y1_4, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_y2_1, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_y2_2, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_y2_3, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_y2_4, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_y2_5, 'read_type': SupportedFileReadType.DATA }
        ] 
        self.task_function = self._task
        self.enduses_needed = [
            'Heating',  'Cooling', 'Ventilation', 'WaterHeating', 'Cooking', 'Refrigeration', 'ExteriorLighting', 
            'InteriorLighting', 'Electronics', 'Appliances', 'Miscellaneous', 'Vehicle'
        ]
        self.unneded_columns = ['Total', 'Service', 'Panel']

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()
        
        self.device_map = data_map[self.input_artifact_device_map]
        self.excluded_locations = data_map[self.input_artifact_excluded_locations]['Residential']
   
        rbsa_dict = {} # device name to enduse
        enduses = set()

        for index, row in self.device_map.iterrows():
            key = row["enduse_code"]
            enduse = row["eu"]
            units = row["units"]
            rbsa_dict[key] = {"enduse": enduse, "unit": units}
            if rbsa_dict[key]['unit'] == 'kWh':
                enduses.add(rbsa_dict[key]['enduse'])

        master_y1 = pd.DataFrame()

        for filename in self.Y1_files:
            logger.info(f'Cleaning {filename}')
            # clean and resample
            df = data_map[filename]
            df['time'] = pd.to_datetime(df['time'], format='%d%b%y:%H:%M:%S')
            df = df.fillna(0)
            df['siteid'] = df['siteid'].astype(str)  
            df = df.groupby('siteid').resample('60T', on='time').sum()
            
            # generate mapping
            column_map = {k: [] for k in enduses}

            for column in list(df): 
                col_name = column.split()[0]

                if (col_name == 'Hours') or (column[:10] == 'Fahrenheit'):
                    continue  
                if rbsa_dict[col_name]['unit'] == 'kWh':    
                    column_map[rbsa_dict[col_name]['enduse']].append(column)
            
            # remove unused
            column_map.pop('', None)
            column_map.pop('ignore', None)
            
            for enduse in column_map.keys():
                df[enduse] = df[column_map[enduse]].sum(axis=1)
                
            df = df[list(column_map.keys())]
            master_y1 = pd.concat([master_y1, df])

        master_y2 = pd.DataFrame()

        for filename in self.Y2_files:
            logger.info(f'Cleaning {filename}')
            # clean and resample
            df = data_map[filename]
            df['time'] = pd.to_datetime(df['time'], format='%d%b%y:%H:%M:%S')
            df = df.fillna(0)
            df['siteid'] = df['siteid'].astype(str)  
            df = df.groupby('siteid').resample('60T', on='time').sum()

            # generate mapping
            column_map = {k: [] for k in enduses}

            for column in df.columns:
                try:
                    if rbsa_dict[column]['unit'] == 'kWh':    
                        column_map[rbsa_dict[column]['enduse']].append(column)
                except KeyError:
                    continue # unused columns
                
            # remove unused
            column_map.pop('', None)
            column_map.pop('ignore', None)

            for enduse in column_map.keys():
                df[enduse] = df[column_map[enduse]].sum(axis=1)
                
            df = df[list(column_map.keys())]
            master_y2 = pd.concat([master_y2, df])

        master_df = pd.concat([master_y1, master_y2])
        master_df = master_df.sort_values(by='siteid')
        master_df = master_df.reset_index(level=[0,1])
        master_df = master_df.groupby('siteid').resample('60T', on='time').sum()

        for enduse in self.enduses_needed:
            if enduse not in master_df.columns:
                master_df[enduse] = 0
                logger.info(f'Adding {enduse} column with values set to 0.')

        master_df = master_df.drop(self.unneded_columns, axis=1)

        logger.info(f'Removing sites {self.excluded_locations["sites"]}.')       
        master_df = master_df.loc[~master_df.index.get_level_values('siteid').isin(self.excluded_locations['sites'])]

        self.validate(master_df)
        self.on_complete({self.output_artifact_clean_data: master_df})           

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
