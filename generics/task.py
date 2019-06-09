import logging
from generics import data_manager


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class Task(data_manager.DataManager):
    """
    Generic task manager that implements a function and executes it on
    a set of given inputs and generates a set of outputs.

    Inputs may be data files
    Outputs may also be data files
    """
    def __init__(self, name):
        self.name = name

    def run():
        print(f'running task {self.name}')
