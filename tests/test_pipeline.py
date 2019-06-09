import unittest
from generics import pipeline as p, task as t

class TestLctkTask(unittest.TestCase):

    def test_pipeline_only_accepts_task_instance(self):
        # random inital test... 
        name = 'I am not of type <Task>!!!'
        pipe = p.Pipeline()
        with self.assertRaises(TypeError):
            pipe.add_task(name)
