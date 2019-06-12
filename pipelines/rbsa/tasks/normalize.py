import logging
from generics import task as t


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class Normalizer(t.Task):
    def __init__(self, name):
        super().__init__(self)
        self.name = name
        self.my_data_files = ['local_data/test.csv', 'config/DEVICE_MAP.json']
        self.task_function = self._task
        self.df = None

    def _save_data(self):
        self.save_data(self.df)

    def _get_data(self):
        self.df = self.load_data(self.my_data_files)

    def _task(self):
        self._get_data()
        logger.info(self.df)
        return 2 * 10 * 50
    