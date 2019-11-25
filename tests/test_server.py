import unittest
from settings import base

class TestServerSetup(unittest.TestCase):

    def test_server_build(self):
        self.assertEqual(base.DEBUG, False)
