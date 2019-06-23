import uuid
import logging
from generics import task as t


logger = logging.getLogger('LCTK_APPLICATION_LOGGER')


class Pipeline(object):
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
            self.name = name 
        self.tasks = []
        self.result_map = {}

    def add_task(self, entry):
        """
        Add a task to the pipeline
        """
        if isinstance(entry, t.Task):
            self.tasks.append(entry)
        else:
            raise TypeError('LoadInsight does not support pipeline execution of tasks that are not an instance of <Task>')
    
    def run(self, **kwargs):
        """
        Run the tasks in a pipeline
        """
        for pipeline_task in self.tasks:
            pipeline_task.run()
            result = pipeline_task.run_result

            logger.info(f'Result of task {pipeline_task.name} is {result} and its execution time is {pipeline_task.get_task_run_time()}')
            logger.info(f'Task finished, is it valid? {str(pipeline_task.did_task_pass_validation)}')

            if not pipeline_task.did_task_pass_validation:
                raise ValueError(f'Validation Failed for task {pipeline_task.name}')
            
            self.result_map[pipeline_task.name] = pipeline_task.task_results
        logging.info('PIPELINE RUN IS DONE')

    def save(self):
        """
        Save the results from a pipeline run to the remote path
        """
        pass

    def cleanup(self, delete_local):
        """
        Cleanup after a pipeline run
        """
        pass
