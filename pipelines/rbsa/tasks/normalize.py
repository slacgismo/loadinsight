import logging
from generics import task as t


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class Normalizer(t.Task):
    def run(self):
        return 2 * 10 * 50
    