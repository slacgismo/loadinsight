import os
import logging
from time import time
from settings import base
from generics import pipeline as p, task as t

from pipelines.ceus.tasks import (
    undiscount_gas, 
    normalize_totals,
    # find_sensitivities,
    fcz_correlation,
    # project_loadshapes,
    # discount_gas,
    # normalize_loadshapes
)


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class CeusPipeline():
    def __init__(self, pipeline_configuration=None):
        self.name = 'loadinsight_ceus_pipeline'
        self.pipeline = p.Pipeline(self.name)
        self.dir_name = f'{base.LOCAL_PATH}/{time()}__{self.name}'
        
        if pipeline_configuration:
            # TODO: establish a configuration scheme for this to run dynamically
            pass
        else:
            self.create_tasks()

    def create_tasks(self):
        undiscount_gas_task = undiscount_gas.UndiscountGas('undiscount_gas_task')
        self.pipeline.add_task(undiscount_gas_task)

        normalize_totals_task = normalize_totals.NormalizeTotals('normalize_totals_task')
        self.pipeline.add_task(normalize_totals_task)

        correlation_task = fcz_correlation.FczCorrelation('correlation_task')
        self.pipeline.add_task(correlation_task)

        # find_sensitivities_task = find_sensitivities.FindSensitivities('find_sensitivities_task')
        # self.pipeline.add_task(find_sensitivities_task)

        # project_loadshapes_task = project_loadshapes.ProjectLoadshapes('project_loadshapes_task')
        # self.pipeline.add_task(project_loadshapes_task)

        # discount_gas_task = discount_gas.DiscountGas('discount_gas_task')
        # self.pipeline.add_task(discount_gas_task)

        # normalize_loadshapes_task = normalize_loadshapes.NormalizeLoadshapes('normalize_loadshapes_task')
        # self.pipeline.add_task(normalize_loadshapes_task)

    def _create_results_storage(self, storage_name=None):
        try:
            if storage_name:
                os.makedirs(storage_name)
            else:
                os.makedirs(self.dir_name)
        except FileExistsError:
            logger.exception(f'Directory we attempted to create for {self.name} already exists')

    def generate_result_plots(self):
        ######## FOR DEBUG PURPOSES!
        import numpy as np
        import matplotlib.pyplot as plt
        from generics.artifact import ArtifactDataManager
        from generics.file_type_enum import SupportedFileReadType

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
