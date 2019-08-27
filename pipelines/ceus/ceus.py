import os
import logging
from time import time
from settings import base
from generics import pipeline as p, task as t

from pipelines.ceus.tasks import (
    undiscount_gas, 
    normalize_totals,
    find_sensitivities,
    fcz_correlation,
    project_loadshapes,
    discount_gas,
    normalize_loadshapes,
    apply_roa
)


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class CeusPipeline():
    def __init__(self, pipeline_configuration=None):
        self.name = 'ceus'
        self.pipeline = p.Pipeline(self.name)
        
        # specify the logical directory structure for this pipeline execution
        self.artifact_root_dir = 'ceus'
        self.artifact_noaa_dir = 'ceus_noaa'
        self.artifact_tmy_base_dir = 'ceus_tmy_base'
        self.artifact_tmy_target_dir = 'ceus_tmy_target'
        self.artifact_target_weather_dir = 'target_weather'

        # the local directory where all the output images are saved for this pipeline run
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
        
        # check for the artifact sub dirs
        if not os.path.isdir(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_noaa_dir}'):
            self._create_results_storage(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_noaa_dir}')
        
        if not os.path.isdir(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_tmy_base_dir}'):
            self._create_results_storage(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_tmy_base_dir}')
        
        if not os.path.isdir(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_tmy_target_dir}'):
            self._create_results_storage(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_tmy_target_dir}')
        
        if not os.path.isdir(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_target_weather_dir}'):
            self._create_results_storage(f'{base.LOCAL_PATH}/{self.artifact_root_dir}/{self.artifact_target_weather_dir}')

    def create_tasks(self):
        undiscount_gas_task = undiscount_gas.UndiscountGas('undiscount_gas_task', self.artifact_root_dir)
        self.pipeline.add_task(undiscount_gas_task)

        normalize_totals_task = normalize_totals.NormalizeTotals('normalize_totals_task', self.artifact_root_dir)
        self.pipeline.add_task(normalize_totals_task)

        correlation_task = fcz_correlation.FczCorrelation('correlation_task', self.artifact_root_dir)
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

    def generate_result_plots(self):
        ######## FOR DEBUG PURPOSES!
        import numpy as np
        import matplotlib.pyplot as plt
        from generics.artifact import ArtifactDataManager
        from generics.file_type_enum import SupportedFileReadType

        adm = ArtifactDataManager()
        
        df =  adm.load_data([
            { 'name': f'{self.artifact_root_dir}/ceus_normal_loadshapes.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/ceus_enduse_loadshapes.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/ceus_total_loadshapes.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/ceus_loadshapes.csv', 'read_type': SupportedFileReadType.DATA },
            { 'name': f'{self.artifact_root_dir}/ceus_components.csv', 'read_type': SupportedFileReadType.DATA }
        ])

        normal_loadshapes = df[f'{self.artifact_root_dir}/ceus_normal_loadshapes.csv']
        enduse_loadshapes = df[f'{self.artifact_root_dir}/ceus_enduse_loadshapes.csv']
        total_loadshapes = df[f'{self.artifact_root_dir}/ceus_total_loadshapes.csv']
        loadshapes = df[f'{self.artifact_root_dir}/ceus_loadshapes.csv']
        components = df[f'{self.artifact_root_dir}/ceus_components.csv']

        base_enduses = list(normal_loadshapes.columns)
        base_enduses.remove('time')
        base_enduses.remove('target')
        base_enduses.remove('buildingtype')
        base_enduses.remove('daytype')
        base_enduses.remove('Heating')
        base_enduses.remove('Cooling')
        ticks = np.arange(0, 25, 3) 

        normal_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/ceus_normal_loadshapes'
        self._create_results_storage(normal_plots_dir)

        enduse_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/ceus_enduse_loadshapes'
        self._create_results_storage(enduse_plots_dir)

        total_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/ceus_total_loadshapes'
        self._create_results_storage(total_plots_dir)

        loadshapes_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/ceus_loadshapes'
        self._create_results_storage(loadshapes_plots_dir)

        components_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/ceus_components'
        self._create_results_storage(components_plots_dir)

        plotting_components = ['PE', 'Stat_P_Cur', 'Stat_P_Res', 'MotorC', 'MotorB', 'MotorA', 'MotorD'] # bottom up

        logger.info('GENERATING CEUS NORMAL LOADSHAPE PLOTS')

        image_index = 0
        for idx, city in enumerate(normal_loadshapes.target.unique()):
            city_df = normal_loadshapes.loc[normal_loadshapes.target == city]
            for zdx, buildingtype in enumerate(normal_loadshapes.buildingtype.unique()):
                buildingtype_df = city_df.loc[city_df.buildingtype == buildingtype]
                max_total = buildingtype_df[['Heating', 'Cooling'] + base_enduses].sum(axis=1).max()
                max_val = 1 if max_total <= 1 else int(max_total) + 1
                for ydx, daytype in enumerate(buildingtype_df.daytype.unique()):
                    title = f'{str(city)}-{str(buildingtype)}-{str(daytype)}'
                    day_df = buildingtype_df.loc[buildingtype_df.daytype == daytype]
                    day_df = day_df.append(day_df.iloc[0])
                    day_df = day_df.reset_index()
                    day_df['Baseload'] = day_df[base_enduses].sum(axis=1)
                    plot = day_df[['Baseload', 'Heating', 'Cooling']].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['black','red','blue'])
                    fig = plot.get_figure()
                    image_index_based_name = '{0:0=4d}'.format(image_index)
                    fig.savefig(f'{normal_plots_dir}/{image_index_based_name}.png')
                    plt.close(fig)
                    image_index += 1

        logger.info('GENERATING CEUS ENDUSE LOADSHAPE PLOTS')

        image_index = 0
        for idx, city in enumerate(enduse_loadshapes.target.unique()):
            city_df = enduse_loadshapes.loc[enduse_loadshapes.target == city]
            for zdx, buildingtype in enumerate(enduse_loadshapes.buildingtype.unique()):
                buildingtype_df = city_df.loc[city_df.buildingtype == buildingtype]
                max_total = buildingtype_df[['Heating', 'Cooling'] + base_enduses].sum(axis=1).max()
                max_val = 1 if max_total <= 1 else int(max_total) + 1
                for ydx, daytype in enumerate(buildingtype_df.daytype.unique()):
                    title = f'{str(city)}-{str(buildingtype)}-{str(daytype)}'
                    day_df = buildingtype_df.loc[buildingtype_df.daytype == daytype]
                    day_df = day_df.append(day_df.iloc[0])
                    day_df = day_df.reset_index()
                    day_df['Baseload'] = day_df[base_enduses].sum(axis=1)
                    plot = day_df[['Baseload', 'Heating', 'Cooling']].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['black','red','blue'])
                    plt.xlabel('Hour-of-Day')
                    plt.ylabel('Load (pu. summer total peak)')
                    fig = plot.get_figure()
                    image_index_based_name = '{0:0=4d}'.format(image_index)
                    fig.savefig(f'{enduse_plots_dir}/{image_index_based_name}.png')
                    plt.close(fig)
                    image_index += 1

        logger.info('GENERATING CEUS TOTAL LOADSHAPE PLOTS')

        image_index = 0
        for idx, city in enumerate(total_loadshapes.target.unique()):
            city_df = total_loadshapes.loc[total_loadshapes.target == city]
            for zdx, buildingtype in enumerate(total_loadshapes.buildingtype.unique()):
                buildingtype_df = city_df.loc[city_df.buildingtype == buildingtype]
                max_total = buildingtype_df[['Heating', 'Cooling'] + base_enduses].sum(axis=1).max()
                max_val = 1 if max_total <= 1 else int(max_total) + 1
                for ydx, daytype in enumerate(buildingtype_df.daytype.unique()):
                    title = f'{str(city)}-{str(buildingtype)}-{str(daytype)}'
                    day_df = buildingtype_df.loc[buildingtype_df.daytype == daytype]
                    day_df = day_df.append(day_df.iloc[0])
                    day_df = day_df.reset_index()
                    day_df['Baseload'] = day_df[base_enduses].sum(axis=1)
                    plot = day_df[['Baseload', 'Heating', 'Cooling']].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['black','red','blue'])
                    plt.xlabel('Hour-of-Day')
                    plt.ylabel('Load (pu. base total peak)')      
                    fig = plot.get_figure()
                    image_index_based_name = '{0:0=4d}'.format(image_index)
                    fig.savefig(f'{total_plots_dir}/{image_index_based_name}.png')
                    plt.close(fig)
                    image_index += 1

        logger.info('GENERATING CEUS LOADSHAPE PLOTS')

        image_index = 0
        for idx, city in enumerate(loadshapes.fcz.unique()):
            city_df = loadshapes.loc[loadshapes.fcz == city]
            for zdx, buildingtype in enumerate(loadshapes.buildingtype.unique()):
                buildingtype_df = city_df.loc[city_df.buildingtype == buildingtype]
                max_total = buildingtype_df[['Heating', 'Cooling'] + base_enduses].sum(axis=1).max()
                max_val = 1 if max_total <= 1 else int(max_total) + 1
                title = f'{str(city)}-{str(buildingtype)}'
                buildingtype_df['Baseload'] = buildingtype_df[base_enduses].sum(axis=1)
                buildingtype_df = buildingtype_df.iloc[:24]
                buildingtype_df = buildingtype_df.reset_index()
                plot = buildingtype_df[['Baseload', 'Heating', 'Cooling']].plot(title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['black','red','blue'])
                plt.xlabel('Hour-of-Day')
                plt.ylabel('Load (pu. base total peak)')   
                fig = plot.get_figure()
                image_index_based_name = '{0:0=4d}'.format(image_index)
                fig.savefig(f'{loadshapes_plots_dir}/{image_index_based_name}.png')
                plt.close(fig)
                image_index += 1    

        logger.info('GENERATING CEUS COMPONENT PLOTS')

        image_index = 0
        for idx, city in enumerate(components.target.unique()):
            city_df = components.loc[components.target == city]
            for zdx, buildingtype in enumerate(loadshapes.buildingtype.unique()):
                buildingtype_df = city_df.loc[city_df.buildingtype == buildingtype]
                max_total = buildingtype_df[plotting_components].sum(axis=1).max()
                max_val = 1 if max_total <= 1 else int(max_total) + 1
                for ydx, daytype in enumerate(buildingtype_df.daytype.unique()):
                    title = f'{str(city)}-{buildingtype}-{str(daytype)}'
                    day_df = buildingtype_df.loc[buildingtype_df.daytype == daytype]
                    day_df = day_df.append(day_df.iloc[0])
                    day_df = day_df.reset_index()
                    plot = day_df[plotting_components].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['green','yellow','brown','blue','grey','black','red'])
                    plt.xlabel('Hour-of-Day')
                    plt.ylabel('Load (pu. summer total peak)')   
                    fig = plot.get_figure()
                    image_index_based_name = '{0:0=4d}'.format(image_index)
                    fig.savefig(f'{components_plots_dir}/{image_index_based_name}.png')
                    plt.close(fig)
                    image_index += 1   

        # fixed image name plots

        normal_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/ceus_normal_loadshapes_named'
        self._create_results_storage(normal_plots_dir)

        enduse_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/ceus_enduse_loadshapes_named'
        self._create_results_storage(enduse_plots_dir)

        total_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/ceus_total_loadshapes_named'
        self._create_results_storage(total_plots_dir)

        loadshapes_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/ceus_loadshapes_named'
        self._create_results_storage(loadshapes_plots_dir)

        components_plots_dir = f'{base.LOCAL_PATH}/{self.run_dir}/ceus_components_named'
        self._create_results_storage(components_plots_dir)

        plotting_components = ['PE', 'Stat_P_Cur', 'Stat_P_Res', 'MotorC', 'MotorB', 'MotorA', 'MotorD'] # bottom up

        logger.info('GENERATING CEUS NORMAL LOADSHAPE PLOTS')

        for idx, city in enumerate(normal_loadshapes.target.unique()):
            city_df = normal_loadshapes.loc[normal_loadshapes.target == city]
            for zdx, buildingtype in enumerate(normal_loadshapes.buildingtype.unique()):
                buildingtype_df = city_df.loc[city_df.buildingtype == buildingtype]
                max_total = buildingtype_df[['Heating', 'Cooling'] + base_enduses].sum(axis=1).max()
                max_val = 1 if max_total <= 1 else int(max_total) + 1
                for ydx, daytype in enumerate(buildingtype_df.daytype.unique()):
                    title = f'CEUS-{str(city).split(",")[0]}_{str(city).split(",")[1]}-{str(buildingtype)}-{str(daytype)}'
                    day_df = buildingtype_df.loc[buildingtype_df.daytype == daytype]
                    day_df = day_df.append(day_df.iloc[0])
                    day_df = day_df.reset_index()
                    day_df['Baseload'] = day_df[base_enduses].sum(axis=1)
                    plot = day_df[['Baseload', 'Heating', 'Cooling']].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['black','red','blue'])
                    fig = plot.get_figure()
                    fig.savefig(f'{normal_plots_dir}/{title}.png')
                    plt.close(fig)

        logger.info('GENERATING CEUS ENDUSE LOADSHAPE PLOTS')

        for idx, city in enumerate(enduse_loadshapes.target.unique()):
            city_df = enduse_loadshapes.loc[enduse_loadshapes.target == city]
            for zdx, buildingtype in enumerate(enduse_loadshapes.buildingtype.unique()):
                buildingtype_df = city_df.loc[city_df.buildingtype == buildingtype]
                max_total = buildingtype_df[['Heating', 'Cooling'] + base_enduses].sum(axis=1).max()
                max_val = 1 if max_total <= 1 else int(max_total) + 1
                for ydx, daytype in enumerate(buildingtype_df.daytype.unique()):
                    title = f'CEUS-{str(city).split(",")[0]}_{str(city).split(",")[1]}-{str(buildingtype)}-{str(daytype)}'
                    day_df = buildingtype_df.loc[buildingtype_df.daytype == daytype]
                    day_df = day_df.append(day_df.iloc[0])
                    day_df = day_df.reset_index()
                    day_df['Baseload'] = day_df[base_enduses].sum(axis=1)
                    plot = day_df[['Baseload', 'Heating', 'Cooling']].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['black','red','blue'])
                    plt.xlabel('Hour-of-Day')
                    plt.ylabel('Load (pu. summer total peak)')
                    fig = plot.get_figure()
                    fig.savefig(f'{enduse_plots_dir}/{title}.png')
                    plt.close(fig)

        logger.info('GENERATING CEUS TOTAL LOADSHAPE PLOTS')

        for idx, city in enumerate(total_loadshapes.target.unique()):
            city_df = total_loadshapes.loc[total_loadshapes.target == city]
            for zdx, buildingtype in enumerate(total_loadshapes.buildingtype.unique()):
                buildingtype_df = city_df.loc[city_df.buildingtype == buildingtype]
                max_total = buildingtype_df[['Heating', 'Cooling'] + base_enduses].sum(axis=1).max()
                max_val = 1 if max_total <= 1 else int(max_total) + 1
                for ydx, daytype in enumerate(buildingtype_df.daytype.unique()):
                    title = f'CEUS-{str(city).split(",")[0]}_{str(city).split(",")[1]}-{str(buildingtype)}-{str(daytype)}'
                    day_df = buildingtype_df.loc[buildingtype_df.daytype == daytype]
                    day_df = day_df.append(day_df.iloc[0])
                    day_df = day_df.reset_index()
                    day_df['Baseload'] = day_df[base_enduses].sum(axis=1)
                    plot = day_df[['Baseload', 'Heating', 'Cooling']].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['black','red','blue'])
                    plt.xlabel('Hour-of-Day')
                    plt.ylabel('Load (pu. base total peak)')      
                    fig = plot.get_figure()
                    fig.savefig(f'{total_plots_dir}/{title}.png')
                    plt.close(fig)

        logger.info('GENERATING CEUS LOADSHAPE PLOTS')

        for idx, city in enumerate(loadshapes.fcz.unique()):
            city_df = loadshapes.loc[loadshapes.fcz == city]
            for zdx, buildingtype in enumerate(loadshapes.buildingtype.unique()):
                buildingtype_df = city_df.loc[city_df.buildingtype == buildingtype]
                max_total = buildingtype_df[['Heating', 'Cooling'] + base_enduses].sum(axis=1).max()
                max_val = 1 if max_total <= 1 else int(max_total) + 1
                title = f'CEUS-{str(city).split(",")[0]}_{str(city).split(",")[1]}-{str(buildingtype)}'
                buildingtype_df['Baseload'] = buildingtype_df[base_enduses].sum(axis=1)
                buildingtype_df = buildingtype_df.iloc[:24]
                buildingtype_df = buildingtype_df.reset_index()
                plot = buildingtype_df[['Baseload', 'Heating', 'Cooling']].plot(title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['black','red','blue'])
                plt.xlabel('Hour-of-Day')
                plt.ylabel('Load (pu. base total peak)')   
                fig = plot.get_figure()
                fig.savefig(f'{loadshapes_plots_dir}/{title}.png')
                plt.close(fig)

        logger.info('GENERATING CEUS COMPONENT PLOTS')

        for idx, city in enumerate(components.target.unique()):
            city_df = components.loc[components.target == city]
            for zdx, buildingtype in enumerate(loadshapes.buildingtype.unique()):
                buildingtype_df = city_df.loc[city_df.buildingtype == buildingtype]
                max_total = buildingtype_df[plotting_components].sum(axis=1).max()
                max_val = 1 if max_total <= 1 else int(max_total) + 1
                for ydx, daytype in enumerate(buildingtype_df.daytype.unique()):
                    title = f'CEUS-{str(city).split(",")[0]}_{str(city).split(",")[1]}-{str(buildingtype)}-{str(daytype)}'
                    day_df = buildingtype_df.loc[buildingtype_df.daytype == daytype]
                    day_df = day_df.append(day_df.iloc[0])
                    day_df = day_df.reset_index()
                    plot = day_df[plotting_components].plot(kind='area', title=title, grid=True, xticks=ticks, ylim=(0, max_val), linewidth=2, color=['green','yellow','brown','blue','grey','black','red'])
                    plt.xlabel('Hour-of-Day')
                    plt.ylabel('Load (pu. summer total peak)')   
                    fig = plot.get_figure()
                    fig.savefig(f'{components_plots_dir}/{title}.png')
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
