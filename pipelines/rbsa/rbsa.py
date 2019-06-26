import os
import logging
from time import time
from settings import base
from generics import pipeline as p, task as t

from pipelines.rbsa.tasks import (
    group_sites, 
    undiscount_gas, 
    index_heatcool, 
    normalize_totals,
    find_sensitivities,
    zipcode_correlation,
    project_loadshapes,
    discount_gas,
    normalize_loadshapes
)


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class RbsaPipeline():
    def __init__(self, pipeline_configuration=None):
        self.name = 'loadinsight_rbsa_pipeline'
        self.pipeline = p.Pipeline(self.name)
        self.dir_name = f'{base.LOCAL_PATH}/{time()}__{self.name}'
        
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

        project_loadshapes_task = project_loadshapes.ProjectLoadshapes('project_loadshapes_task')
        self.pipeline.add_task(project_loadshapes_task)

        discount_gas_task = discount_gas.DiscountGas('discount_gas_task')
        self.pipeline.add_task(discount_gas_task)

        normalize_loadshapes_task = normalize_loadshapes.NormalizeLoadshapes('normalize_loadshapes_task')
        self.pipeline.add_task(normalize_loadshapes_task)

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

        adm = ArtifactDataManager()
        
        df =  adm.load_data([
            { 'name': 'normal_loadshapes.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'enduse_loadshapes.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'total_loadshapes.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'loadshapes.csv', 'read_type': SupportedFileReadType.DATA }
        ])

        normal_loadshapes = df['normal_loadshapes.csv']
        enduse_loadshapes = df['enduse_loadshapes.csv']
        total_loadshapes = df['total_loadshapes.csv']
        loadshapes = df['loadshapes.csv']

        base_enduses = list(normal_loadshapes.columns)
        base_enduses.remove('time')
        base_enduses.remove('target')
        base_enduses.remove('daytype')
        base_enduses.remove('Heating')
        base_enduses.remove('Cooling')
        ticks = np.arange(0, 25, 3) 

        normal_plots_dir = f'{self.dir_name}/normal_loadshapes'
        self._create_results_storage(normal_plots_dir)

        enduse_plots_dir = f'{self.dir_name}/enduse_loadshapes'
        self._create_results_storage(enduse_plots_dir)

        total_plots_dir = f'{self.dir_name}/total_loadshapes'
        self._create_results_storage(total_plots_dir)

        loadshapes_plots_dir = f'{self.dir_name}/loadshapes'
        self._create_results_storage(loadshapes_plots_dir)

        logger.info('GENERATING NORMAL LOADSHAPE PLOTS')

        image_index = 0
        for idx, city in enumerate(normal_loadshapes.target.unique()):
            city_df = normal_loadshapes.loc[normal_loadshapes.target == city]
            max_total = city_df[['Heating', 'Cooling'] + base_enduses].sum(axis=1).max()
            max_val = 1 if max_total <= 1 else int(max_total) + 1
            for ydx, daytype in enumerate(city_df.daytype.unique()):
                title = f'{str(city)}-{str(daytype)}'
                day_df = city_df.loc[city_df.daytype == daytype]
                day_df = day_df.append(day_df.iloc[0])
                day_df = day_df.reset_index()
                day_df['Baseload'] = day_df[base_enduses].sum(axis=1)
                plot = day_df[['Baseload', 'Heating', 'Cooling']].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['black','red','blue'])
                plt.xlabel('Hour-of-Day')
                plt.ylabel('Load (pu. summer total peak)')
                fig = plot.get_figure()
                image_index_based_name = '{0:0=2d}'.format(image_index)
                fig.savefig(f'{normal_plots_dir}/{image_index_based_name}.png')
                plt.close(fig)
                image_index += 1

        logger.info('GENERATING ENDUSE LOADSHAPE PLOTS')

        image_index = 0
        for idx, city in enumerate(enduse_loadshapes.target.unique()):
            city_df = enduse_loadshapes.loc[enduse_loadshapes.target == city]
            max_total = city_df[['Heating', 'Cooling'] + base_enduses].sum(axis=1).max()
            max_val = 1 if max_total <= 1 else int(max_total) + 1
            for ydx, daytype in enumerate(city_df.daytype.unique()):
                title = f'{str(city)}-{str(daytype)}'
                day_df = city_df.loc[city_df.daytype == daytype]
                day_df = day_df.append(day_df.iloc[0])
                day_df = day_df.reset_index()
                day_df['Baseload'] = day_df[base_enduses].sum(axis=1)
                plot = day_df[['Baseload', 'Heating', 'Cooling']].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['black','red','blue'])
                plt.xlabel('Hour-of-Day')
                plt.ylabel('Load (pu. base total peak)')      
                fig = plot.get_figure()
                image_index_based_name = '{0:0=2d}'.format(image_index)
                fig.savefig(f'{enduse_plots_dir}/{image_index_based_name}.png')
                plt.close(fig)
                image_index += 1

        logger.info('GENERATING TOTAL LOADSHAPE PLOTS')

        image_index = 0
        for idx, city in enumerate(total_loadshapes.target.unique()):
            city_df = total_loadshapes.loc[total_loadshapes.target == city]
            max_total = city_df[['Heating', 'Cooling'] + base_enduses].sum(axis=1).max()
            max_val = 1 if max_total <= 1 else int(max_total) + 1
            for ydx, daytype in enumerate(city_df.daytype.unique()):
                title = f'{str(city)}-{str(daytype)}'
                day_df = city_df.loc[city_df.daytype == daytype]
                day_df = day_df.append(day_df.iloc[0])
                day_df = day_df.reset_index()
                day_df['Baseload'] = day_df[base_enduses].sum(axis=1)
                plot = day_df[['Baseload', 'Heating', 'Cooling']].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['black','red','blue'])
                plt.xlabel('Hour-of-Day')
                plt.ylabel('Load (pu. base total peak)')   
                fig = plot.get_figure()
                image_index_based_name = '{0:0=2d}'.format(image_index)
                fig.savefig(f'{total_plots_dir}/{image_index_based_name}.png')
                plt.close(fig)
                image_index += 1

        logger.info('GENERATING LOADSHAPE PLOTS')

        image_index = 0
        for idx, city in enumerate(loadshapes.zipcode.unique()):
            city_df = loadshapes.loc[loadshapes.zipcode == city]
            max_total = city_df[['Heating', 'Cooling'] + base_enduses].sum(axis=1).max()
            max_val = 1 if max_total <= 1 else int(max_total) + 1
            title = city
            city_df['Baseload'] = city_df[base_enduses].sum(axis=1)
            city_df = city_df.iloc[:24]
            city_df = city_df.reset_index()
            plot = city_df[['Baseload', 'Heating', 'Cooling']].plot(title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['black','red','blue'])
            plt.xlabel('Hour-of-Day')
            plt.ylabel('Load (pu. base total peak)')   
            fig = plot.get_figure()
            image_index_based_name = '{0:0=2d}'.format(image_index)
            fig.savefig(f'{loadshapes_plots_dir}/{image_index_based_name}.png')
            plt.close(fig)
            image_index += 1      

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
