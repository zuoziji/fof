import unittest
import sys
sys.path.append('..')
from  fof_app import create_app
from fof_app.models import db
from flask import current_app
import os


class BasicsTestCase(unittest.TestCase):
    def setUp(self):
        env = os.environ.get('APP_ENV', 'testing')
        self.app = create_app('fof_app.config.%sConfig' %env.capitalize())
        self.app_context = self.app.app_context()
        self.app_context.push()

    def tearDown(self):
        db.session.remove()
        self.app_context.pop()

    def test_app_exists(self):
        self.assertFalse(current_app is None)

    def test_app_is_testing(self):
        self.assertTrue(current_app.config['TESTING'])


if __name__ == "__main__":
    unittest.main()