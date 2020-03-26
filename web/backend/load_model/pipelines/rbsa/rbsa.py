import os
import logging
from time import time
from load_model.settings import base
from load_model.generics import pipeline as p, task as t
from backend.helpers import s3_helper
import numpy as np
import matplotlib.pyplot as plt

from load_model.pipelines.rbsa.tasks import (
    apply_devicemap,
    group_sites,
    undiscount_gas,
    index_heatcool,
    normalize_totals,
    find_sensitivities,
    zipcode_correlation,
    project_loadshapes,
    discount_gas,
    normalize_loadshapes,
    apply_roa
)


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class RbsaPipeline():
    def __init__(self, pipeline_configuration=None, execution_id=None):
        self.name = 'rbsa'
        self.pipeline = p.Pipeline(self.name)

        # specify the logical directory structure for this pipeline execution
        self.artifact_root_dir = 'rbsa'
        self.artifact_raw_dir = 'raw'
        self.artifact_noaa_dir = 'noaa'
        self.artifact_tmy_base_dir = 'tmy_base'
        self.artifact_tmy_target_dir = 'tmy_target'
        self.artifact_target_weather_dir = 'target_weather'
        # the local directory where all the output images are saved for this pipeline run
        # change the run dir name: add user_id in the dir
        self.run_dir = f'{execution_id}__{time()}__{self.name}'
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

        # check for the artifact sub dirs
        if not os.path.isdir(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_raw_dir}'):
            self._create_results_storage(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_raw_dir}')

        if not os.path.isdir(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_noaa_dir}'):
            self._create_results_storage(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_noaa_dir}')

        if not os.path.isdir(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_tmy_base_dir}'):
            self._create_results_storage(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_tmy_base_dir}')

        if not os.path.isdir(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_tmy_target_dir}'):
            self._create_results_storage(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_tmy_target_dir}')

        if not os.path.isdir(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_target_weather_dir}'):
            self._create_results_storage(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_target_weather_dir}')

    def create_tasks(self):

        apply_devicemap_task = apply_devicemap.ApplyDevicemap('apply_devicemap_task', self.artifact_root_dir)
        self.pipeline.add_task(apply_devicemap_task)

        site_grouping_task = group_sites.SitesGrouper('site_grouping_task', self.artifact_root_dir)
        self.pipeline.add_task(site_grouping_task)

        heatcool_indexing_task = index_heatcool.HeatcoolIndexer('heatcool_indexing_task', self.artifact_root_dir)
        self.pipeline.add_task(heatcool_indexing_task)

        undiscount_gas_task = undiscount_gas.UndiscountGas('undiscount_gas_task', self.artifact_root_dir)
        self.pipeline.add_task(undiscount_gas_task)

        normalize_totals_task = normalize_totals.NormalizeTotals('normalize_totals_task', self.artifact_root_dir)
        self.pipeline.add_task(normalize_totals_task)

        correlation_task = zipcode_correlation.ZipcodeCorrelation('correlation_task', self.artifact_root_dir)
        self.pipeline.add_task(correlation_task)

        find_sensitivities_task = find_sensitivities.FindSensitivities('find_sensitivities_task', self.artifact_root_dir)
        self.pipeline.add_task(find_sensitivities_task)

        project_loadshapes_task = project_loadshapes.ProjectLoadshapes('project_loadshapes_task', self.artifact_root_dir)
        self.pipeline.add_task(project_loadshapes_task)

        discount_gas_task = discount_gas.DiscountGas('discount_gas_task', self.artifact_root_dir)
        self.pipeline.add_task(discount_gas_task)

        normalize_loadshapes_task = normalize_loadshapes.NormalizeLoadshapes('normalize_loadshapes_task', self.artifact_root_dir)
        self.pipeline.add_task(normalize_loadshapes_task)

        apply_roa_task = apply_roa.ApplyRoa('apply_roa_task', self.artifact_root_dir)
        self.pipeline.add_task(apply_roa_task)

    def _create_results_storage(self, storage_name=None):
        try:
            if storage_name:
                os.makedirs(storage_name)
            else:
                os.makedirs(self.dir_name)
        except FileExistsError:
            logger.exception(f'Directory we attempted to create for {self.name} already exists')
        except OSError:
            logger.exception(f'Failed creating directory {storage_name}')

    def generate_result_plots(self):
        ######## FOR DEBUG PURPOSES!
        import numpy as np
        import matplotlib.pyplot as plt
        from load_model.generics.artifact import ArtifactDataManager
        from load_model.generics.file_type_enum import SupportedFileReadType

        adm = ArtifactDataManager()

        df =  adm.load_data([
            { 'name': f'{self.artifact_root_dir}/normal_loadshapes.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/enduse_loadshapes.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/total_loadshapes.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/loadshapes.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/components.csv', 'read_type': SupportedFileReadType.DATA }
        ])

        normal_loadshapes = df[f'{self.artifact_root_dir}/normal_loadshapes.csv']
        enduse_loadshapes = df[f'{self.artifact_root_dir}/enduse_loadshapes.csv']
        total_loadshapes = df[f'{self.artifact_root_dir}/total_loadshapes.csv']
        loadshapes = df[f'{self.artifact_root_dir}/loadshapes.csv']
        components = df[f'{self.artifact_root_dir}/components.csv']

        base_enduses = list(normal_loadshapes.columns)
        base_enduses.remove('time')
        base_enduses.remove('target')
        base_enduses.remove('daytype')
        base_enduses.remove('Heating')
        base_enduses.remove('Cooling')

        plotting_components = ['PE', 'Stat_P_Cur', 'Stat_P_Res', 'MotorC', 'MotorB', 'MotorA', 'MotorD'] # bottom up
        self.ticks = np.arange(0, 25, 3)

        normal_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/normal_loadshapes'
        s3_normal_plots_dir = f'{self.run_dir}/normal_loadshapes'
        self._create_results_storage(normal_plots_dir)

        enduse_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/enduse_loadshapes'
        s3_enduse_plots_dir = f'{self.run_dir}/enduse_loadshapes'
        self._create_results_storage(enduse_plots_dir)

        total_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/total_loadshapes'
        s3_total_plots_dir = f'{self.run_dir}/total_loadshapes'
        self._create_results_storage(total_plots_dir)

        loadshapes_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/loadshapes'
        s3_loadshapes_plots_dir = f'{self.run_dir}/loadshapes'
        self._create_results_storage(loadshapes_plots_dir)

        components_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/components'
        s3_components_plots_dir = f'{self.run_dir}/components'
        self._create_results_storage(components_plots_dir)

        logger.info('GENERATING RBSA NORMAL LOADSHAPE PLOTS')
        self.loadshapes_plotting(loadshapes=normal_loadshapes, directory=normal_plots_dir, s3_directory=s3_normal_plots_dir, base_enduses=base_enduses)

        logger.info('GENERATING RBSA ENDUSE LOADSHAPE PLOTS')
        self.loadshapes_plotting(loadshapes=enduse_loadshapes, directory=enduse_plots_dir, s3_directory=s3_enduse_plots_dir, base_enduses=base_enduses)

        logger.info('GENERATING RBSA TOTAL LOADSHAPE PLOTS')
        self.loadshapes_plotting(loadshapes=total_loadshapes, directory=total_plots_dir, s3_directory=s3_total_plots_dir, base_enduses=base_enduses)

        logger.info('GENERATING RBSA LOADSHAPE PLOTS')

        for idx, city in enumerate(loadshapes.zipcode.unique()):
            city_df = loadshapes.loc[loadshapes.zipcode == city]
            max_total = city_df[['Heating', 'Cooling'] + base_enduses].sum(axis=1).max()
            max_val = 1 if max_total <= 1 else int(max_total) + 1
            title = f'RBSA-{str(city)}'
            city_df['Baseload'] = city_df[base_enduses].sum(axis=1)
            city_df = city_df.iloc[:24]
            city_df = city_df.append(city_df.iloc[0])
            city_df = city_df.reset_index()
            plot = city_df[['Baseload', 'Heating', 'Cooling']].plot(title=title, grid=True, xticks=self.ticks, ylim=(0, max_val), linewidth=2, color=['black','red','blue'])
            plt.xlabel('Hour-of-Day')
            plt.ylabel('Load (pu. base total peak)')
            fig = plot.get_figure()
            local_file_name = f'{loadshapes_plots_dir}/{title}.png'
            fig.savefig(local_file_name)
            s3_helper.upload_file(local_file_name, base.S3_OUTPUT_BUCKET_PATH, f'{s3_loadshapes_plots_dir}/{title}.png')
            plt.close(fig)

        logger.info('GENERATING RBSA COMPONENT PLOTS')

        for idx, city in enumerate(components.target.unique()):
            city_df = components.loc[components.target == city]
            max_total = city_df[plotting_components].sum(axis=1).max()
            max_val = 1 if max_total <= 1 else int(max_total) + 1
            for ydx, daytype in enumerate(city_df.daytype.unique()):
                title = f'RBSA-{str(city).split(",")[0]}_{str(city).split(",")[1]}-{str(daytype)}'
                day_df = city_df.loc[city_df.daytype == daytype]
                day_df = day_df.append(day_df.iloc[0])
                day_df = day_df.reset_index()
                plot = day_df[plotting_components].plot(kind='area', title=title, grid=True, xticks=self.ticks, ylim=(0, max_val), linewidth=2, color=['green','yellow','brown','blue','grey','black','red'])
                plt.xlabel('Hour-of-Day')
                plt.ylabel('Load (pu. summer total peak)')
                fig = plot.get_figure()
                local_file_name = f'{components_plots_dir}/{title}.png'
                fig.savefig(local_file_name)
                s3_helper.upload_file(local_file_name, base.S3_OUTPUT_BUCKET_PATH, f'{s3_components_plots_dir}/{title}.png')
                plt.close(fig)

    def loadshapes_plotting(self, loadshapes, directory, s3_directory, base_enduses):
        ######## Plotting helper function
        for idx, city in enumerate(loadshapes.target.unique()):
            city_df = loadshapes.loc[loadshapes.target == city]
            max_total = city_df[['Heating', 'Cooling'] + base_enduses].sum(axis=1).max()
            max_val = 1 if max_total <= 1 else int(max_total) + 1
            for ydx, daytype in enumerate(city_df.daytype.unique()):
                title = f'RBSA-{str(city).split(",")[0]}_{str(city).split(",")[1]}-{str(daytype)}'
                day_df = city_df.loc[city_df.daytype == daytype]
                day_df = day_df.append(day_df.iloc[0])
                day_df = day_df.reset_index()
                day_df['Baseload'] = day_df[base_enduses].sum(axis=1)
                plot = day_df[['Baseload', 'Heating', 'Cooling']].plot(kind='area', title=title, grid=True, xticks=self.ticks, ylim=(0, max_val), linewidth=2, color=['black','red','blue'])
                plt.xlabel('Hour-of-Day')
                plt.ylabel('Load (pu. summer total peak)')
                fig = plot.get_figure()
                local_file_name = f'{directory}/{title}.png'
                fig.savefig(local_file_name)
                s3_helper.upload_file(local_file_name, base.S3_OUTPUT_BUCKET_PATH, f'{s3_directory}/{title}.png')
                plt.close(fig)

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

    def on_failure(self):
        logger.info('Performing pipeline cleanup')
