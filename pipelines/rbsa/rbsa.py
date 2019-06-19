import logging
from generics import pipeline as p, task as t

from pipelines.rbsa.tasks import (
    group_sites, 
    undiscount_gas, 
    index_heatcool, 
    normalize_totals,
    find_sensitivities,
    zipcode_correlation
)

logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class RbsaPipeline():
    def __init__(self, pipeline_configuration=None):
        self.name = 'lctk_rbsa_pipeline'
        self.pipeline = p.Pipeline(self.name)
        
        if pipeline_configuration:
            # TODO: establish a configuration scheme for this to run dynamically
            pass
        else:
            self.create_tasks()

    def create_tasks(self):
        site_grouping_task = group_sites.SitesGrouper('site_grouping_task')
        self.pipeline.add_task(site_grouping_task)

        heatcool_indexing_task = index_heatcool.HeatcoolIndexer('heatcool_indexing_task')
        self.pipeline.add_task(heatcool_indexing_task)

        undiscount_gas_task = undiscount_gas.UndiscountGas('undiscount_gas_task')
        self.pipeline.add_task(undiscount_gas_task)

        normalize_totals_task = normalize_totals.NormalizeTotals('normalize_totals_task')
        self.pipeline.add_task(normalize_totals_task)

        correlation_task = zipcode_correlation.ZipcodeCorrelation('correlation_task')
        self.pipeline.add_task(correlation_task)

        find_sensitivities_task = find_sensitivities.FindSensitivities('find_sensitivities_task')
        self.pipeline.add_task(find_sensitivities_task)

    def execute(self):
        """
        Run all the tasks in this pipeline
        """
        try:
            self.pipeline.run()
            logger.info(self.pipeline.result_map)
        except ValueError as ve:
            logger.exception(f'{self.name} failed its pipeline execution. Cleaning up and exiting')
            self.on_failure()

    def on_failure(self):
        logger.info('Performing pipeline cleanup')
