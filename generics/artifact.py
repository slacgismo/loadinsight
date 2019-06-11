import logging


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class ArtifactDataManager(object):
    def __init__(self):
        print("I'm a new artifact data manager")

    def load_data(self, data_files):
        data_map = {}
        for filename in data_files:
            logger.info(filename)
            data_map[filename] = 1234
        return data_map
        
class ArtifactDataWrapper(object):
    def __init__(self):
        print("I'm a new artifact data wrapper")