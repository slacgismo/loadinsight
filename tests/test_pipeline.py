import unittest
from generics import pipeline as p, task as t

class TestLctkTask(unittest.TestCase):

    def test_pipeline_only_accepts_task_instance(self):
        name = 'I am not of type <Task>!!!'
        pipe = p.Pipeline()

        with self.assertRaises(TypeError):
            pipe.add_task(name)

    def test_pipeline_raises_on_run_failure(self):
        task = t.Task('Failing Task')
        task.did_task_pass_validation = False
        task.task_function = lambda: None

        pipe = p.Pipeline()
        pipe.add_task(task)

        with self.assertRaises(ValueError):
            pipe.run()
