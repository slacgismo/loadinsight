import os
import logging
from time import time
from settings import base
from generics import pipeline as p, task as t
import numpy as np
import matplotlib.pyplot as plt

from pipelines.mix.tasks import (
    get_mixed
)


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class MixedFeederPipeline():
    def __init__(self, pipeline_configuration=None):
        self.name = 'mixed_commercial_residential'
        self.pipeline = p.Pipeline(self.name)

        self.artifact_root_dir = 'mix'
        self.run_dir = f'{time()}__{self.name}'

        if pipeline_configuration:
            # TODO: establish a configuration scheme for this to run dynamically
            pass
        else:
            self.create_tasks()

        self._verify_or_create_local_artifact_directory()

    def _verify_or_create_local_artifact_directory(self):
        # check if an rbsa artifact folder exists in the local data
        if not os.path.isdir(f'{base.LOCAL_PATH}/{self.artifact_root_dir}'):
            self._create_results_storage(f'{base.LOCAL_PATH}/{self.artifact_root_dir}')

        # create the unique run folder for this run instance
        self._create_results_storage(f'{base.LOCAL_PATH}/{self.run_dir}')

    def create_tasks(self):
        get_mixed_task = get_mixed.GetMixed('get_mixed_task', self.artifact_root_dir)
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
            self.pipeline.run()
            logger.info(f'Total Pipeline Run Time: {self.pipeline.total_pipeline_run_time}')
        except ValueError as ve:
            logger.exception(f'{self.name} failed its pipeline execution. Cleaning up and exiting')
            self.on_failure()

    def generate_result_plots(self):
        ######## FOR DEBUG PURPOSES!
        from generics.artifact import ArtifactDataManager
        from generics.file_type_enum import SupportedFileReadType

        adm = ArtifactDataManager()

        df =  adm.load_data([
            { 'name': f'{self.artifact_root_dir}/suburban_mix_output.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/urban_mix_output.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/mixed_mix_output.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/rural_mix_output.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/suburban_mix_output_hour_norm.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/urban_mix_output_hour_norm.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/mixed_mix_output_hour_norm.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/rural_mix_output_hour_norm.csv', 'read_type': SupportedFileReadType.DATA }
        ])

        residential_mix = df[f'{self.artifact_root_dir}/suburban_mix_output.csv']
        commercial_mix = df[f'{self.artifact_root_dir}/urban_mix_output.csv']
        mixed_mix = df[f'{self.artifact_root_dir}/mixed_mix_output.csv']
        rural_mix = df[f'{self.artifact_root_dir}/rural_mix_output.csv']

        residential_mix_hour_norm = df[f'{self.artifact_root_dir}/suburban_mix_output_hour_norm.csv']
        commercial_mix_hour_norm = df[f'{self.artifact_root_dir}/urban_mix_output_hour_norm.csv']
        mixed_mix_hour_norm = df[f'{self.artifact_root_dir}/mixed_mix_output_hour_norm.csv']
        rural_mix_hour_norm = df[f'{self.artifact_root_dir}/rural_mix_output_hour_norm.csv']


        plot_csv_dir = f'{base.LOCAL_PATH}/{self.run_dir}/plot_csv'
        self._create_results_storage(plot_csv_dir)
        self.plot_csv_dir = plot_csv_dir

        residential_mix_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/suburban_mix'
        self._create_results_storage(residential_mix_plots_dir)

        commercial_mix_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/urban_mix'
        self._create_results_storage(commercial_mix_plots_dir)

        mixed_mix_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/mixed_mix'
        self._create_results_storage(mixed_mix_plots_dir)

        rural_mix_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/rural_mix'
        self._create_results_storage(rural_mix_plots_dir)

        residential_mix_hour_norm_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/suburban_mix_hour_norm'
        self._create_results_storage(residential_mix_hour_norm_plots_dir)

        commercial_mix_hour_norm_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/urban_mix_hour_norm'
        self._create_results_storage(commercial_mix_hour_norm_plots_dir)

        mixed_mix_hour_norm_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/mixed_mix_hour_norm'
        self._create_results_storage(mixed_mix_hour_norm_plots_dir)

        rural_mix_hour_norm_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/rural_mix_hour_norm'
        self._create_results_storage(rural_mix_hour_norm_plots_dir)

        logger.info('GENERATING SUBURBAN MIX PLOTS')
        self.mix_type_plotting(mix_type=residential_mix, mix_type_name='SUBURBAN', normalization='summer_total_peak', directory=residential_mix_plots_dir)

        logger.info('GENERATING URBAN MIX PLOTS')
        self.mix_type_plotting(mix_type=commercial_mix, mix_type_name='URBAN', normalization='summer_total_peak', directory=commercial_mix_plots_dir)

        logger.info('GENERATING MIXED MIX PLOTS')
        self.mix_type_plotting(mix_type=mixed_mix, mix_type_name='MIXED', normalization='summer_total_peak', directory=mixed_mix_plots_dir)

        logger.info('GENERATING RURAL MIX PLOTS')
        self.mix_type_plotting(mix_type=rural_mix, mix_type_name='RURAL', normalization='summer_total_peak', directory=rural_mix_plots_dir)

        logger.info('GENERATING hour_norm SUBURBAN MIX PLOTS')
        self.mix_type_plotting(mix_type=residential_mix_hour_norm, mix_type_name='SUBURBAN', normalization='hour_norm', directory=residential_mix_hour_norm_plots_dir)

        logger.info('GENERATING hour_norm URBAN MIX PLOTS')
        self.mix_type_plotting(mix_type=commercial_mix_hour_norm, mix_type_name='URBAN', normalization='hour_norm', directory=commercial_mix_hour_norm_plots_dir)

        logger.info('GENERATING hour_norm MIXED MIX PLOTS')
        self.mix_type_plotting(mix_type=mixed_mix_hour_norm, mix_type_name='MIXED', normalization='hour_norm', directory=mixed_mix_hour_norm_plots_dir)

        logger.info('GENERATING hour_norm RURAL MIX PLOTS')
        self.mix_type_plotting(mix_type=rural_mix_hour_norm, mix_type_name='RURAL', normalization='hour_norm', directory=rural_mix_hour_norm_plots_dir)

    def mix_type_plotting(self, mix_type, mix_type_name, normalization, directory):
        ######## Plotting helper function
        csv_components = ['target', 'mix', 'daytype', 'PE', 'Stat_P_Cur', 'Stat_P_Res', 'MotorC', 'MotorB', 'MotorA', 'MotorD']
        plotting_components = ['PE', 'Stat_P_Cur', 'Stat_P_Res', 'MotorC', 'MotorB', 'MotorA', 'MotorD'] # bottom up
        ticks = np.arange(0, 25, 3)

        for idx, city in enumerate(mix_type.target.unique()):
            city_df = mix_type.loc[mix_type.target == city]
            max_total = city_df[plotting_components].sum(axis=1).max()
            max_val = 1 if max_total <= 1 else int(max_total) + 1
            for ydx, daytype in enumerate(city_df.daytype.unique()):
                title = f'{mix_type_name}-{normalization}-{str(city).split(",")[0]}_{str(city).split(",")[1]}-{str(daytype)}'
                day_df = city_df.loc[city_df.daytype == daytype]
                day_df = day_df.append(day_df.iloc[0])
                day_df = day_df.reset_index()
                day_df[csv_components].to_csv(f'{self.plot_csv_dir}/{title}.csv', index=False)
                plot = day_df[plotting_components].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, 1.2), linewidth=2, color=['green', 'yellow', 'brown', 'blue', 'grey', 'black', 'red'])
                plt.xlabel('Hour-of-Day')
                plt.ylabel('Load (pu. {normalization})')
                fig = plot.get_figure()
                fig.savefig(f'{directory}/{title}.png')
                plt.close(fig)

    def on_failure(self):
        logger.info('Performing pipeline cleanup')
