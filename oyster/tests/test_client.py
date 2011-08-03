from unittest import TestCase

import pymongo
from oyster.client import Client


class ClientTests(TestCase):

    def setUp(self):
        self.client = Client()

    def test_constructor(self):
        c = Client('127.0.0.1', 27017, 'testdb', mongo_log_maxsize=5000,
                   user_agent='test-ua', rpm=30, timeout=60,
                   retry_attempts=1, retry_wait_seconds=10)
        assert c.db.connection.host == '127.0.0.1'
        assert c.db.connection.port == 27017
        assert c.db.logs.options()['capped'] == True
        assert c.db.logs.options()['size'] == 5000
        assert c.scraper.user_agent == 'test-ua'
        assert c.scraper.requests_per_minute == 30
        assert c.scraper.timeout == 60
        assert c.scraper.retry_attempts == 1
        assert c.scraper.retry_wait_seconds == 10

    def test_log(self):
        self.client._wipe()
        self.client.log('action1', 'http://example.com')
        self.client.log('action2', 'http://test.com', error=True, pi=3.14)
        assert self.client.db.logs.count() == 2
        x = self.client.db.logs.find_one({'error': True})
        assert x['action'] == 'action2'
        assert x['url'] == 'http://test.com'
        assert x['pi'] == 3.14

