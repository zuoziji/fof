import unittest
import sys

sys.path.append('..')
from fof_app import create_app
from fof_app.models import db
from fof_app.models import FUND_TRANSACTION
import os
import json


class BasicsTestCase(unittest.TestCase):
    def setUp(self):
        env = os.environ.get('APP_ENV', 'testing')
        self.app = create_app('fof_app.config.%sConfig' % env.capitalize())
        self.app_context = self.app.app_context()
        self.app_context.push()
        self.client = self.app.test_client(use_cookies=True)

    def tearDown(self):
        db.session.remove()
        self.app_context.pop()

    def login(self, email, password):
        return self.client.post('/login', data=dict(
            email=email,
            password=password
        ), follow_redirects=True)

    @unittest.skip("skip")
    def test_login(self):
        rv = self.login("kuangmsn@163.com", "123")
        self.assertTrue("基金" in rv.get_data(as_text=True))

    @unittest.skip("skip")
    def test_home_page(self):
        self.login("kuangmsn@163.com", "123")
        response = self.client.get("/f_app/")
        self.assertTrue("基金" in response.get_data(as_text=True))

    @unittest.skip("skip")
    def test_transaction_page(self):
        self.login("kuangmsn@163.com", "123")
        rv = self.client.get('/f_app/transaction')
        self.assertTrue(rv.status_code == 200)

    def test_del_transaction(self):
        self.login("kuangmsn@163.com", "123")
        tr = FUND_TRANSACTION.query.all()
        id_list = [i.id for i in tr]
        headers = {
            'Content-Type': 'application/json',
        }
        rv = self.client.post('/f_app/del_transaction', data=json.dumps({"result":id_list}), headers=headers)
        status = rv.data.decode('utf8')
        self.assertEqual("ok", json.loads(status)['status'])


if __name__ == "__main__":
    unittest.main()
