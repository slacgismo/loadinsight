import uuid
import logging
from generics import task as t


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class Pipeline():
    """
    LCTK pipeline implementation.
    Takes in a given number of Task objects and executes their function
    in the given order. The pipeline is in charge of managing the overall
    atomicity of the pipeline execution and the success/failure rollback 
    protocol, to include global setup and cleanup.
    """
    def __init__(self, name=None):
        """
        Constructor for a generic pipeline
        name <string>: optional name for the pipeline
        """
        if not name:
            # w/o a name, generate a random UUID to name this pipeline
            self.name = str(uuid.uuid4())
        else:
            # we currently don't mitigate name conflicts 
            self.name = name 
        self.tasklist = []
        self.datalist = {}
        # global cachename
        # cachename = name + "/"

    def add_task(self, entry):
        """
        Add a task to the pipeline
        """
        if isinstance(entry, t.Task):
            self.tasklist.append(entry)
        else:
            raise TypeError('LCTK does not support pipeline execution of tasks that are not an instance of <Task>')

    def run(self, **kwargs):
        """
        Run the tasks in a pipeline
        """
        for fn in self.tasklist:
            fn.run()
        # todo: run them in dependency order, in parallel,
        #       and skip unneeded updates
        # for task in self.tasklist:
        #     if hasattr(task,"inputs"):
        #         readall(task.inputs)
        #     setall(task.outputs,pd.DataFrame())
        #     task.run()
        #     writeall(task.outputs)
        #     if hasattr(task,"check"):
        #         task.check()

    def save(self):
        """
        Save the results from a pipeline run to the remote path
        """
        pass
        # if config.SAVE_DATA:
        #     for key,info in cache.items():
        #         import shutil
        #         shutil.copyfile(local_path(info["hash"]),remote_path(key))

    def cleanup(self, delete_local):
        """
        Cleanup after a pipeline run
        """
        # global cache
        # verbose("cleaning up", context(class_name=__class__.__name__))
        # for key,info in cache.items():
        #     filename = local_path(info["hash"])
        #     if delete_local and os.path.exists(filename):
        #         os.remove(filename)
        #     if config.USE_CACHE and "data" in info.keys():
        #         del info["data"]

    def set_tasks(self, tasklist=[]):
        self.tasklist = tasklist

    def add_data(self, **kwargs):
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
        """
        Run the tasks in a pipeline
        """
        for current_task in self.tasklist:
            current_task.run()
            result = current_task.run_result
            logger.info(f'Result of task {current_task.name} is {result} and its execution time is {current_task.get_task_run_time()}')
        # todo: run them in dependency order, in parallel,
        #       and skip unneeded updates
        # for task in self.tasklist:
        #     if hasattr(task,"inputs"):
        #         readall(task.inputs)
        #     setall(task.outputs,pd.DataFrame())
        #     task.run()
        #     writeall(task.outputs)
        #     if hasattr(task,"check"):
        #         task.check()

    # def plot(self,**kwargs):
    #     for name,data in self.datalist.items():
    #         data.plot(name,**kwargs)