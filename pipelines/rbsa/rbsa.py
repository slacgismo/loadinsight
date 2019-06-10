import logging
from generics import pipeline as p, task as t


class RbsaPipeline(p.Pipeline):
    def __init__(self):
        self.pipeline = p.Pipeline('lctk_rbsa_pipeline')
        
        # FIXME: we can likely pull this from a config...
        self.create_tasks()

    def create_tasks(self):
        # these will come from other defintions
        fake_task = t.Task('fake_task')
        self.pipeline.add_task(fake_task)

    def execute(self):
        """
        Run all the tasks in this pipeline
        """
        self.pipeline.run()
