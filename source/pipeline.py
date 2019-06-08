from utils import *

config = load_config()

class pipeline:
    """Pipeline implementation"""
    def __init__(self,name=""):
        """Create a new pipeline"""
        self.tasklist = []
        self.datalist = []
        global cachename
        cachename = name + "/"

    def save(self):
        """Save the results from a pipeline run to the remote path"""
        if config.SAVE_DATA:
            for key,info in cache.items():
                import shutil
                shutil.copyfile(local_path(info["hash"]),remote_path(key))

    def cleanup(self,delete_local=config.CLEAN_LOCAL):
        """Cleanup after a pipeline run"""
        global cache
        verbose("cleaning cache")
        for key,info in cache.items():
            filename = local_path(info["hash"])
            if delete_local and os.path.exists(filename):
                os.remove(filename)
            if config.USE_CACHE:
                del info["data"]

    def add_task(self,entry):
        """Add a task to the pipeline"""
        self.tasklist.append(entry)

    def set_tasks(self,tasklist = []):
        self.tasklist = tasklist

    def add_data(self,name):
        item = data(name)
        self.datalist.append(item)
        return item

    def set_data(self,datalist = []):
        self.datalist = []
        for data in datalist:
            self.add_data(data)
        return self.datalist

    def run(self):
        """Run the tasks in a pipeline"""
        # todo: run them in dependency order, in parallel,
        #       and skip unneeded updates
        for task in self.tasklist:
            task.run()
