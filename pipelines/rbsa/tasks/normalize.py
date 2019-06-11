import logging
from generics import task as t


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class Normalizer(t.Task):
    def __init__(self, name):
        super().__init__(self)
        self.name = name
        self.task_function = self._task

    def _task(self):
        logger.info(self.data)
        return 2 * 10 * 50
    