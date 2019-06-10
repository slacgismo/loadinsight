import logging
from pipelines.rbsa.tasks import normalize
from generics import pipeline as p, task as t


class RbsaPipeline(p.Pipeline):
    def __init__(self):
        self.pipeline = p.Pipeline('lctk_rbsa_pipeline')
        
        # FIXME: we can likely pull this from a config...
        self.create_tasks()

    def create_tasks(self):
        multiply_task = normalize.Normalizer('multiply_task')
        self.pipeline.add_task(multiply_task)

    def execute(self):
        """
        Run all the tasks in this pipeline
        """
        self.pipeline.run()
