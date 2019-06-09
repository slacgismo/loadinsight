import unittest
from settings import base

class TestLctkSetup(unittest.TestCase):

    def test_debug_is_off_by_default(self):
        self.assertEqual(base.DEBUG, False)
