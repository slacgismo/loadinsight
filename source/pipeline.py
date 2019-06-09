"""Implement a task pipeline which operates on data objects

Usage:
    from utils import *
    config = load_config("my_config")
    import pipeline, task, check, plot
    my_pipe = pipeline.pipeline(name="selftest")
    my_input = my_pipe.add_data(name="my_input")
    my_input = my_pipe.add_data(name="my_input", check=check.my_data, plot=plot.my_plot)
    my_pipe.add_task(task.my_task(args={"inputs":[my_input],"outputs":[my_output]}))
    my_pipe.run()
    my_pipe.save()
    my_pipe.plot() 
    my_pipe.cleanup()
"""

from utils import *

config = load_config()

class pipeline:
    """Pipeline implementation"""

    def __init__(self,name=""):
        """Create a new pipeline"""
        self.name = name
        self.tasklist = []
        self.datalist = {}
        self.cache = safecache(name)

    def save(self):
        """Save the results from a pipeline run to the remote path"""
        if config.SAVE_DATA:
            for key,info in self.cache.items():
                import shutil
                shutil.copyfile(local_path(info["hash"]),remote_path(key))

    def cleanup(self):
        """Cleanup after a pipeline run"""
        verbose("cleaning up", context(class_name=self.name))
        self.cache.clean(delete_local=config.CLEAN_LOCAL)

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
            if hasattr(task,"check"):
                task.check()

    def plot(self,**kwargs):
        for name,data in self.datalist.items():
            data.plot(name,**kwargs)
