from utils import *

config = load_config()

class pipeline:
    """Pipeline implementation"""
    def __init__(self,name=""):
        """Create a new pipeline"""
        self.tasklist = []
        self.datalist = {}
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
        verbose("cleaning up", context(class_name=__class__.__name__))
        for key,info in cache.items():
            filename = local_path(info["hash"])
            if delete_local and os.path.exists(filename):
                os.remove(filename)
            if config.USE_CACHE and "data" in info.keys():
                del info["data"]

    def add_task(self,entry):
        """Add a task to the pipeline"""
        self.tasklist.append(entry)

    def set_tasks(self,tasklist = []):
        self.tasklist = tasklist

    def add_data(self,**kwargs):
        item = data(**kwargs)
        self.datalist[kwargs["name"]] = item
        return item

    def set_data(self,datalist = []):
        self.datalist = {}
        for data in datalist:
            self.add_data(data)
        return self.datalist

    def get_data(self,name):
        return self.datalist[name]

    def run(self,**kwargs):
        """Run the tasks in a pipeline"""
        # todo: run them in dependency order, in parallel,
        #       and skip unneeded updates
        for task in self.tasklist:
            if hasattr(task,"inputs"):
                readall(task.inputs)
            setall(task.outputs,pd.DataFrame())
            task.run()
            writeall(task.outputs)
            task.check()

