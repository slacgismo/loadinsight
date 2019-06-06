import unittest
from init import DEBUG

class TestLctkSetup(unittest.TestCase):

    def test_debug_is_off_by_default(self):
        self.assertEquals(DEBUG, False)
