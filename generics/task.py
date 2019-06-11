import logging
from time import time
from generics import artifact


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class Task(artifact.ArtifactDataManager):
    """
    Generic task manager that implements a function and executes it on
    a set of given inputs and generates a set of outputs.

    Inputs may be data files
    Outputs may also be data files
    """
    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.run_result = None
        self.task_function = None
        self.task_end_time = None
        self.task_start_time = None

    def _get_time(self):
        return time()

    def get_task_run_time(self):
        try:
            return (self.task_end_time - self.task_start_time) / 60.
        except TypeError as e:
            logger.exception('Could not deduce task run time')
            return 0

    def run(self):
        # set the start time for this run
        self.task_start_time = self._get_time()

        logger.info(f'running task {self.name}')
        if self.task_function:
            self.run_result = self.task_function()
        else:
            raise TypeError(f'Task {self.name} does not implement a function that this pipeline can execute')

        # set the end time for this run
        self.task_end_time = self._get_time()

