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

    def generate_result_plots(self):
        ######## FOR DEBUG PURPOSES!
        import numpy as np
        import matplotlib.pyplot as plt
        from generics.artifact import ArtifactDataManager
        from generics.file_type_enum import SupportedFileReadType

        adm = ArtifactDataManager()
        
        df =  adm.load_data([
            { 'name': 'residential_mix_output.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'commercial_mix_output.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'mixed_mix_output.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': 'rural_mix_output.csv', 'read_type': SupportedFileReadType.DATA }
        ])

        residential_mix = df['residential_mix_output.csv']
        commercial_mix = df['commercial_mix_output.csv']
        mixed_mix = df['mixed_mix_output.csv']
        rural_mix = df['rural_mix_output.csv']

        ticks = np.arange(0, 25, 3) 

        plotting_components = ['MotorA', 'MotorB', 'MotorC', 'MotorD', 'PE', 'Stat_P_Cur', 'Stat_P_Res']

        residential_mix_plots_dir = f'{self.dir_name}/residential_mix'
        self._create_results_storage(residential_mix_plots_dir)

        commercial_mix_plots_dir = f'{self.dir_name}/commercial_mix'
        self._create_results_storage(commercial_mix_plots_dir)

        mixed_mix_plots_dir = f'{self.dir_name}/mixed_mix'
        self._create_results_storage(mixed_mix_plots_dir)

        rural_mix_plots_dir = f'{self.dir_name}/rural_mix'
        self._create_results_storage(rural_mix_plots_dir)

        logger.info('GENERATING RESIDENTIAL MIX PLOTS')

        image_index = 0
        for idx, city in enumerate(residential_mix.target.unique()):
            city_df = residential_mix.loc[residential_mix.target == city]
            max_total = city_df[plotting_components].sum(axis=1).max()
            max_val = 1 if max_total <= 1 else int(max_total) + 1
            for ydx, daytype in enumerate(city_df.daytype.unique()):
                title = f'{str(city)}-{str(daytype)}'
                day_df = city_df.loc[city_df.daytype == daytype]
                day_df = day_df.append(day_df.iloc[0])
                day_df = day_df.reset_index()
                plot = day_df[plotting_components].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['green','yellow','brown','blue','grey','black','red'])
                plt.xlabel('Hour-of-Day')
                plt.ylabel('Load (pu. summer total peak)')   
                fig = plot.get_figure()
                image_index_based_name = '{0:0=2d}'.format(image_index)
                fig.savefig(f'{residential_mix_plots_dir}/{image_index_based_name}.png')
                plt.close(fig)
                image_index += 1   

        logger.info('GENERATING COMMERCIAL MIX PLOTS')

        image_index = 0
        for idx, city in enumerate(commercial_mix.target.unique()):
            city_df = commercial_mix.loc[commercial_mix.target == city]
            max_total = city_df[plotting_components].sum(axis=1).max()
            max_val = 1 if max_total <= 1 else int(max_total) + 1
            for ydx, daytype in enumerate(city_df.daytype.unique()):
                title = f'{str(city)}-{str(daytype)}'
                day_df = city_df.loc[city_df.daytype == daytype]
                day_df = day_df.append(day_df.iloc[0])
                day_df = day_df.reset_index()
                plot = day_df[plotting_components].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['green','yellow','brown','blue','grey','black','red'])
                plt.xlabel('Hour-of-Day')
                plt.ylabel('Load (pu. total peak)')   
                fig = plot.get_figure()
                image_index_based_name = '{0:0=2d}'.format(image_index)
                fig.savefig(f'{commercial_mix_plots_dir}/{image_index_based_name}.png')
                plt.close(fig)
                image_index += 1   

        logger.info('GENERATING MIXED MIX PLOTS')

        image_index = 0
        for idx, city in enumerate(mixed_mix.target.unique()):
            city_df = mixed_mix.loc[mixed_mix.target == city]
            max_total = city_df[plotting_components].sum(axis=1).max()
            max_val = 1 if max_total <= 1 else int(max_total) + 1
            for ydx, daytype in enumerate(city_df.daytype.unique()):
                title = f'{str(city)}-{str(daytype)}'
                day_df = city_df.loc[city_df.daytype == daytype]
                day_df = day_df.append(day_df.iloc[0])
                day_df = day_df.reset_index()
                plot = day_df[plotting_components].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['green','yellow','brown','blue','grey','black','red'])
                plt.xlabel('Hour-of-Day')
                plt.ylabel('Load (pu. total peak)')   
                fig = plot.get_figure()
                image_index_based_name = '{0:0=2d}'.format(image_index)
                fig.savefig(f'{mixed_mix_plots_dir}/{image_index_based_name}.png')
                plt.close(fig)
                image_index += 1   

        logger.info('GENERATING RURAL MIX PLOTS')

        image_index = 0
        for idx, city in enumerate(rural_mix.target.unique()):
            city_df = rural_mix.loc[rural_mix.target == city]
            max_total = city_df[plotting_components].sum(axis=1).max()
            max_val = 1 if max_total <= 1 else int(max_total) + 1
            for ydx, daytype in enumerate(city_df.daytype.unique()):
                title = f'{str(city)}-{str(daytype)}'
                day_df = city_df.loc[city_df.daytype == daytype]
                day_df = day_df.append(day_df.iloc[0])
                day_df = day_df.reset_index()
                plot = day_df[plotting_components].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['green','yellow','brown','blue','grey','black','red'])
                plt.xlabel('Hour-of-Day')
                plt.ylabel('Load (pu. total peak)')   
                fig = plot.get_figure()
                image_index_based_name = '{0:0=2d}'.format(image_index)
                fig.savefig(f'{rural_mix_plots_dir}/{image_index_based_name}.png')
                plt.close(fig)
                image_index += 1   

    def on_failure(self):
        logger.info('Performing pipeline cleanup')
