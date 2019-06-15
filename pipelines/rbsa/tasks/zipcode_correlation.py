import logging
import pandas as pd
from generics import task as t
from generics.file_type_enum import SupportedFileReadType


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class ZipcodeCorrelation(t.Task):
    def __init__(self, name):
        super().__init__(self)
        self.name = name
        self.data_files = [
            # { 'name': 'area_loads.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/594.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/596.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/597.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/598.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/833.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/835.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/836.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/837.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/838.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/970.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/971.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/972.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/973.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/974.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/980.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/981.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/982.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/983.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/984.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/985.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/988.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/989.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/990.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/991.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/992.csv', 'read_type': SupportedFileReadType.DATA },
            # { 'name': 'noaa/993.csv', 'read_type': SupportedFileReadType.DATA }
        ]
        self.task_function = self._task
        self.output_artifact_enduse_loads = 'enduse_loads.csv'
        
        self.data_map = None
        self.df = None

        # these should be read from config, they are different for RBSA and CEUS
        self.theat = 15
        self.tcool = 25

    def _save_data(self):
        self.save_data(self.df)

    def _get_data(self):
        self.data_map = self.load_data(self.data_files) 
        self.df = self.data_map['area_loads.csv']        

    def _task(self):
        self._get_data()
        logger.info(self.df)

        self.validate(enduse_loads)
        self.save_data({self.output_artifact_enduse_loads: enduse_loads})

    def validate(self, df):
        """
        Validation
        """
        logger.info(f'Validating task {self.name}')
        if df.isnull().values.any():
            logger.exception(f'Task {self.name} did not pass validation. Error found during grouping of sites to zip codes.')
            self.did_task_pass_validation = False
            self.on_failure()

    def on_failure(self):
        logger.info('Perform task cleanup because we failed')
        super().on_failure()
