import time
import datetime
from unittest import TestCase

from nose.tools import assert_raises
import pymongo

from oyster.client import Client


class ClientTests(TestCase):

    def setUp(self):
        self.client = Client()
        self.client._wipe()


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
        self.client.log('action1', 'http://example.com')
        self.client.log('action2', 'http://test.com', error=True, pi=3)
        assert self.client.db.logs.count() == 2
        x = self.client.db.logs.find_one({'error': True})
        assert x['action'] == 'action2'
        assert x['url'] == 'http://test.com'
        assert x['pi'] == 3


    def test_track_url(self):
        # basic insert
        self.client.track_url('http://example.com', update_mins=30, pi=3)
        obj = self.client.db.tracked.find_one()
        assert '_random' in obj
        assert obj['update_mins'] == 30
        assert obj['metadata'] == {'pi': 3}

        # logging
        log = self.client.db.logs.find_one()
        assert log['action'] == 'track'
        assert log['url'] == 'http://example.com'

        # can't track same URL twice
        assert_raises(ValueError, self.client.track_url, 'http://example.com')

        # logged error
        assert self.client.db.logs.find_one({'error': 'already tracked'})


    def test_md5_versioning(self):
        doc = {'url': 'hello.txt'}
        self.client.fs.put('hello!', filename='hello.txt')
        assert not self.client.md5_versioning(doc, 'hello!')
        assert self.client.md5_versioning(doc, 'hey!')


    def test_update(self):
        # get a single document tracked
        self.client.track_url('http://example.com', update_mins=60, pi=3)
        obj = self.client.db.tracked.find_one()
        self.client.update(obj)

        # check that metadata has been updated
        newobj = self.client.db.tracked.find_one()
        assert (newobj['last_update'] +
                datetime.timedelta(minutes=newobj['update_mins']) ==
                newobj['next_update'])
        first_update = newobj['last_update']
        assert newobj['consecutive_errors'] == 0

        # check that document exists in database
        doc = self.client.fs.get_last_version()
        assert doc.filename == 'http://example.com'
        assert doc.content_type.startswith('text/html')
        assert doc.pi == 3

        # check logs
        assert self.client.db.logs.find({'action': 'update'}).count() == 1

        # and do an update..
        self.client.update(obj)

        # hopefully example.com hasn't changed, this tests that md5 worked
        assert self.client.db.fs.files.count() == 1

        # check that appropriate metadata updated
        newobj = self.client.db.tracked.find_one()
        assert first_update < newobj['last_update']

        # check that logs updated
        assert self.client.db.logs.find({'action': 'update'}).count() == 2


    def test_update_failure(self):
        # track a non-existent URL
        self.client.track_url('http://not_a_url')
        obj = self.client.db.tracked.find_one()
        self.client.update(obj)

        obj = self.client.db.tracked.find_one()
        assert obj['consecutive_errors'] == 1

        # we should have logged an error too
        assert self.client.db.logs.find({'action': 'update',
                                         'error': {'$ne': False}}).count() == 1

        # update again
        self.client.update(obj)

        obj = self.client.db.tracked.find_one()
        assert obj['consecutive_errors'] == 2


    def test_all_versions(self):
        random_url = 'http://en.wikipedia.org/wiki/Special:Random'
        self.client.track_url(random_url)
        obj = self.client.db.tracked.find_one()
        self.client.update(obj)

        versions = self.client.get_all_versions(random_url)
        assert versions[0].filename == random_url

        self.client.update(obj)
        assert len(self.client.get_all_versions(random_url)) == 2


    def test_get_update_queue(self):
        self.client.track_url('never-updates', update_mins=0.01)
        self.client.track_url('fake-1', update_mins=0.01)
        self.client.track_url('fake-2', update_mins=0.01)
        self.client.track_url('fake-3', update_mins=0.01)

        never = self.client.db.tracked.find_one(dict(url='never-updates'))
        fake1 = self.client.db.tracked.find_one(dict(url='fake-1'))
        fake2 = self.client.db.tracked.find_one(dict(url='fake-2'))
        fake3 = self.client.db.tracked.find_one(dict(url='fake-3'))

        # 4 in queue, ordered by random
        queue = self.client.get_update_queue()
        assert len(queue) == 4
        assert queue[0]['_random'] < queue[1]['_random'] < queue[2]['_random']

        # update a few
        self.client.update(fake1)
        self.client.update(fake2)
        self.client.update(fake3)

        # queue should only have never in it
        queue = self.client.get_update_queue()
        assert len(queue) == 1

        # wait for time to pass
        time.sleep(1)

        # queue should be full, but start with the un-updated one
        queue = self.client.get_update_queue()
        assert len(queue) == 4
        assert queue[0]['_id'] == never['_id']


    def test_get_update_queue_size(self):
        self.client.track_url('a', update_mins=0.01)
        self.client.track_url('b', update_mins=0.01)
        self.client.track_url('c', update_mins=0.01)

        a = self.client.db.tracked.find_one(dict(url='a'))
        b = self.client.db.tracked.find_one(dict(url='b'))
        c = self.client.db.tracked.find_one(dict(url='c'))

        assert self.client.get_update_queue_size() == 3

        self.client.update(a)

        assert self.client.get_update_queue_size() == 2

        self.client.update(b)
        self.client.update(c)

        assert self.client.get_update_queue_size() == 0

        # wait
        time.sleep(1)

        assert self.client.get_update_queue_size() == 3
