import unittest
from generics import task as t

class TestLctkTask(unittest.TestCase):

    def test_task_name_is_set(self): 
        name = 'My Random Task'
        task = t.Task(name)
        self.assertEqual(task.name, name)

    def test_task_returns_zero_when_run_time_has_bad_data(self):
        task = t.Task('t-one')
        get_time_result = task.get_task_run_time() 
        self.assertEqual(get_time_result, 0)
            
    def test_task_run_raises_type_error_with_no_function(self):
        task = t.Task('t-two')
        with self.assertRaises(TypeError):
            task.run()

    def test_task_run_executes_function(self):
        task = t.Task('t-three')
        task.task_function = lambda: None
        run_result = task.run()
        self.assertEqual(run_result, None)