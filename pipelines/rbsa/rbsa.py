import logging
from pipelines.rbsa.tasks import normalize, group_sites, index_heatcool
from generics import pipeline as p, task as t


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class RbsaPipeline(p.Pipeline):
    def __init__(self):
        self.name = 'lctk_rbsa_pipeline'
        self.pipeline = p.Pipeline(self.name)
        
        # FIXME: we can likely pull this from a config...
        self.create_tasks()

    def create_tasks(self):

        # site_grouping_task = group_sites.SitesGrouper('site_grouping_task')
        # self.pipeline.add_task(site_grouping_task)

        heatcool_indexing_task = index_heatcool.HeatcoolIndexer('heatcool_indexing_task')
        self.pipeline.add_task(heatcool_indexing_task)

    def execute(self):
        """
        Run all the tasks in this pipeline
        """
        try:
            self.pipeline.run()
        except ValueError as ve:
            logger.exception(f'{self.name} failed its pipeline execution. Cleaning up and exiting')
            self.on_failure()

    def on_failure(self):
        logger.info('Performing pipeline cleanup')
