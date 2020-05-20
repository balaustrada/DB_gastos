import unittest
import os
from modules import db_part as f


class TestModule(unittest.TestCase):
    def test_get_credentials(self):
        self.assertEqual( ("localhost", "pi", "sd23iojrafk", 8888,  "127.0.0.1") , f.get_db_credentials(os.getcwd() + '/db_credentials_example.ini') )