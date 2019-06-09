import unittest
from generics import task as t

class TestLctkTask(unittest.TestCase):

    def test_task_name_is_set(self):
        # random inital test... 
        name = 'My Random Task'
        task = t.Task(name)
        self.assertEqual(task.name, name)
