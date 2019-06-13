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

    def _save_data(self):
        self.save_data(self.df)

    def _get_data(self):
        self.df = self.load_data(self.data_files)['area_loads.csv']
        self.weather = self.load_data(self.noaa_files)

    def _task(self):
        self._get_data()
        logger.info(self.df)

        zipcodes = self.df.zipcode.unique()

        print(self.df.head(10))

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
