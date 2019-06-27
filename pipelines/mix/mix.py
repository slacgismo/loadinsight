import os
import logging
from time import time
from settings import base
from generics import pipeline as p, task as t

from pipelines.mix.tasks import (
    get_mixed
)


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class MixPipeline():
    def __init__(self, pipeline_configuration=None):
        self.name = 'loadinsight_mix_pipeline'
        self.pipeline = p.Pipeline(self.name)
        self.dir_name = f'{base.LOCAL_PATH}/{time()}__{self.name}'
        
        if pipeline_configuration:
            # TODO: establish a configuration scheme for this to run dynamically
            pass
        else:
            self.create_tasks()

    def create_tasks(self):
        get_mixed_task = get_mixed.GetMixed('get_mixed_task')
        self.pipeline.add_task(get_mixed_task)

    def _create_results_storage(self, storage_name=None):
        try:
            if storage_name:
                os.makedirs(storage_name)
            else:
                os.makedirs(self.dir_name)
        except FileExistsError:
            logger.exception(f'Directory we attempted to create for {self.name} already exists')

    def execute(self):
        """
        Run all the tasks in this pipeline
        """
        try:
            self._create_results_storage()
            self.pipeline.run()
            logger.info(f'Total Pipeline Run Time: {self.pipeline.total_pipeline_run_time}')
        except ValueError as ve:
            logger.exception(f'{self.name} failed its pipeline execution. Cleaning up and exiting')
            self.on_failure()

    def on_failure(self):
        logger.info('Performing pipeline cleanup')
