import logging
from generics import artifact


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class Task(artifact.ArtifactDataManager):
    """
    Generic task manager that implements a function and executes it on
    a set of given inputs and generates a set of outputs.

    Inputs may be data files
    Outputs may also be data files
    """
    def __init__(self, name):
        self.name = name
