import logging
import pandas as pd
from generics import task as t
from generics.file_type_enum import SupportedFileReadType
import numpy as np

logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class ApplyRoa(t.Task):
    """ 
    This class is used to group sites into 3 digit zip codes
    """
    def __init__(self, name, pipeline_artifact_dir):
        super().__init__(self)
        self.name = name
        self.input_artifact_roa_com = f'{pipeline_artifact_dir}/roa_com.csv'
        self.input_artifact_normal_loadshapes = f'{pipeline_artifact_dir}/ceus_normal_loadshapes.csv'
        self.output_artifact_components = f'{pipeline_artifact_dir}/ceus_components.csv'
        self.my_data_files = [
            { 'name': self.input_artifact_roa_com, 'read_type': SupportedFileReadType.DATA },
            { 'name': self.input_artifact_normal_loadshapes, 'read_type': SupportedFileReadType.DATA }
        ] 
        self.task_function = self._task

    def _get_data(self):
        return self.load_data(self.my_data_files)
        
    def _task(self):
        data_map = self._get_data()
        
        self.df = data_map[self.input_artifact_normal_loadshapes]
        self.roa = data_map[self.input_artifact_roa_com]

        self.roa = self.roa.set_index('Commercial', drop=True)
        roa_matrix = self.roa.values
        roa_matrix = roa_matrix.astype(float)
        ROA_columns = self.roa.columns
        
        for column in ROA_columns:
            if column not in self.df.columns:
                logger.exception(f'{self.name} failed its pipeline execution. ROA enduse names dont match residential')
                self.on_failure()

        locations = self.df.target.unique()
        buildingtypes = self.df.buildingtype.unique()
        daytypes = self.df.daytype.unique()

        components_df = pd.DataFrame(columns=['target','buildingtype','daytype','time'] + list(self.roa.index))

        for location in locations:
        	for buildingtype in buildingtypes:
	            for daytype in daytypes:
	                day_df = self.df.loc[(self.df.target == location) & (self.df.buildingtype == buildingtype) & (self.df.daytype == daytype)]
	                day_matrix = day_df[ROA_columns].values.astype(float)
	                component_matrix = np.matmul(day_matrix, roa_matrix.T)
	                components_day_df = pd.DataFrame(component_matrix, columns=self.roa.index)
	                components_day_df.insert(loc=0, column='target', value=location)
	                components_day_df.insert(loc=1, column='buildingtype', value=buildingtype)
	                components_day_df.insert(loc=2, column='daytype', value=daytype)
	                components_day_df.insert(loc=3, column='time', value=range(24))
	                components_df = components_df.append(components_day_df)

        self.validate(components_df)
        self.on_complete({self.output_artifact_components: components_df})           

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
