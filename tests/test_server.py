import unittest
import requests
from settings import base

class TestServerSetup(unittest.TestCase):

    def test_server_build(self):
        # test front end page
        response = requests.get('http://localhost:8000')
        self.assertEqual(response.status_code, 200)

        html = response.text
        self.assertEqual('<title>LoadInsight</title>' in html, True)
