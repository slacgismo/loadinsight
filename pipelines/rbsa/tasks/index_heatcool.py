import logging
from generics import task as t
import pandas as pd

logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class HeatcoolIndexer(t.Task):
    def __init__(self, name):
        super().__init__(self)
        self.name = name
        self.noaa_files = ['local_data/rbsa_weather/594.csv',
                            'local_data/rbsa_weather/596.csv',
                            'local_data/rbsa_weather/597.csv',
                            'local_data/rbsa_weather/833.csv',
                            'local_data/rbsa_weather/835.csv',
                            'local_data/rbsa_weather/836.csv',
                            'local_data/rbsa_weather/837.csv',
                            'local_data/rbsa_weather/838.csv',
                            'local_data/rbsa_weather/970.csv',
                            'local_data/rbsa_weather/971.csv',
                            'local_data/rbsa_weather/972.csv',
                            'local_data/rbsa_weather/973.csv',
                            'local_data/rbsa_weather/974.csv',
                            'local_data/rbsa_weather/980.csv',
                            'local_data/rbsa_weather/981.csv',
                            'local_data/rbsa_weather/982.csv',
                            'local_data/rbsa_weather/983.csv',
                            'local_data/rbsa_weather/984.csv',
                            'local_data/rbsa_weather/985.csv',
                            'local_data/rbsa_weather/988.csv',
                            'local_data/rbsa_weather/989.csv',
                            'local_data/rbsa_weather/990.csv',
                            'local_data/rbsa_weather/991.csv',
                            'local_data/rbsa_weather/992.csv',
                            'local_data/rbsa_weather/993.csv'
                            ]
        self.data_files = ['local_data/area_loads.csv']
        self.task_function = self._task
        self.df = None

    def _save_data(self):
        self.save_data(self.df)

    def _get_data(self):
        self.df = self.load_data(self.data_files)['local_data/area_loads.csv']

    def _task(self):
        self._get_data()
        logger.info(self.df)

        print(self.df.zipcode.unique())
        return 2 * 10 * 50
    