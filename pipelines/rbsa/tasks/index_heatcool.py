import logging
from generics import task as t
import pandas as pd

logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class HeatcoolIndexer(t.Task):
    def __init__(self, name):
        super().__init__(self)
        self.name = name
        self.noaa_files = ['rbsa_noaa/594.csv',
                            'rbsa_noaa/596.csv',
                            'rbsa_noaa/597.csv',
                            'rbsa_noaa/598.csv',
                            'rbsa_noaa/833.csv',
                            'rbsa_noaa/835.csv',
                            'rbsa_noaa/836.csv',
                            'rbsa_noaa/837.csv',
                            'rbsa_noaa/838.csv',
                            'rbsa_noaa/970.csv',
                            'rbsa_noaa/971.csv',
                            'rbsa_noaa/972.csv',
                            'rbsa_noaa/973.csv',
                            'rbsa_noaa/974.csv',
                            'rbsa_noaa/980.csv',
                            'rbsa_noaa/981.csv',
                            'rbsa_noaa/982.csv',
                            'rbsa_noaa/983.csv',
                            'rbsa_noaa/984.csv',
                            'rbsa_noaa/985.csv',
                            'rbsa_noaa/988.csv',
                            'rbsa_noaa/989.csv',
                            'rbsa_noaa/990.csv',
                            'rbsa_noaa/991.csv',
                            'rbsa_noaa/992.csv',
                            'rbsa_noaa/993.csv'
                            ]
        self.data_files = ['area_loads.csv']
        self.task_function = self._task
        self.df = None

        # these should be read from config, they are different for RBSA and CEUS
        self.theat = 15
        self.tcool = 25

    def _save_data(self):
        self.save_data(self.df)

    def _get_data(self):
        self.df = self.load_data(self.data_files)['area_loads.csv']
        self.weather = self.load_data(self.noaa_files)

    def _task(self):
        self._get_data()
        logger.info(self.df)

        print(self.df.columns)
        zipcodes = self.df.zipcode.unique()

        for zipcode in zipcodes:
            print(zipcode)
            zipcode_df = self.df.loc[self.df.zipcode == zipcode]
            
            filename = 'rbsa_noaa/'+str(zipcode)+'.csv'
            zipcode_weather = self.weather[filename]
                
            # validation for date ranges of zip codes load data date range to noaa data for that zipcode
            if (zipcode_df.time.max() > zipcode_weather.DATE.max()) | (zipcode_df.time.min() < zipcode_weather.DATE.min()):
                logger.exception(f'Task {self.name} did not pass validation. Error found in matching noaa weather file date range to {zipcode} zip code.')
                self.did_task_pass_validation = False
                self.on_failure()

            print('df', zipcode_df.shape)
            print('weather1', zipcode_weather.shape)

            start = zipcode_df.time.min()
            end = zipcode_df.time.max()

            zipcode_weather = zipcode_weather.loc[(zipcode_weather.DATE >= start) & (zipcode_weather.DATE <= end)]

            print('weather2', zipcode_weather.shape)

            load_df = pd.DataFrame(columns=['HeatCool','Temperature','Indexer','Heating','Cooling','Ventilation'])

            load_df['HeatCool'] = zipcode_df['HeatCool']
            load_df['Temperature'] = zipcode_weather['Temperature']
            load_df['Indexer'] = zipcode_weather.apply(self.temp_dir, axis=1)
            load_df['Heating'] = load_df.apply(self.heatCol, axis=1)
            load_df['Cooling'] = load_df.apply(self.coolCol, axis=1)
            load_df['Ventilation'] = load_df.apply(self.ventCol, axis=1)

            print('load', load_df.shape)

            print(load_df)

    def temp_dir(self, row):
        """Function used for seperating heatcool
        """

        if row['Temperature'] < self.theat:
            val = "Heating"
        elif row['Temperature'] > self.tcool:
            val = "Cooling"
        else:
            val = "Ventilation"
        return val         


    def heatCol(self, row):
        """Function used for seperating heat from heatcool
        """

        if row['Indexer'] == "Heating":
            val = row['HeatCool']
        else:
            val = 0
        return val  


    def coolCol(self, row):
        """Function used for seperating cool from heatcool
        """

        if row['Indexer'] == "Cooling":
            val = row['HeatCool']
        else:
            val = 0
        return val  


    def ventCol(self, row):
        """Function used for seperating cool from heatcool
        """

        if row['Indexer'] == "Ventilation":
            val = row['HeatCool']
        else:
            val = 0
        return val  


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
