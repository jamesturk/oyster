import time
import datetime
from unittest import TestCase

from nose.tools import assert_raises, assert_equal

from oyster.core import Kernel


def hook_fired(doc, newdata):
    doc['hook_fired'] = doc.get('hook_fired', 0) + 1

RANDOM_URL = ('http://www.random.org/integers/?num=1&min=-1000000000&'
              'max=1000000000&col=1&base=10&format=plain&rnd=new')


class KernelTests(TestCase):

    def setUp(self):
        doc_classes = {'default':
                        # omit doc class, defaults to dummy
                        {'update_mins': 30, 'onchanged': [] },
                       'fast-update':
                        {'update_mins': 1 / 60., 'storage_engine': 'dummy',
                         'onchanged': []
                        },
                       'one-time':
                        {'update_mins': None, 'storage_engine': 'dummy',
                         'onchanged': [],
                        },
                       'change-hook':
                        {'update_mins': 30, 'storage_engine': 'dummy',
                         'onchanged': [hook_fired]
                        }
                      }
        self.kernel = Kernel(mongo_db='oyster_test',
                             retry_wait_minutes=1 / 60.,
                             doc_classes=doc_classes)
        self.kernel._wipe()

    def test_constructor(self):
        c = Kernel('127.0.0.1', 27017, 'testdb', mongo_log_maxsize=5000,
                   user_agent='test-ua', rpm=30, timeout=60,
                   retry_attempts=7, retry_wait_minutes=8)
        assert c.db.connection.host == '127.0.0.1'
        assert c.db.connection.port == 27017
        assert c.retry_wait_minutes == 8
        # TODO: test retry_attempts
        assert c.scraper.user_agent == 'test-ua'
        assert c.scraper.requests_per_minute == 30
        assert c.scraper.timeout == 60

        # ensure that a bad document class raises an error
        assert_raises(ValueError, Kernel, doc_classes={'bad-doc': {}})

    def test_track_url(self):
        # basic insert
        id1 = self.kernel.track_url('http://example.com', 'default', pi=3)
        obj = self.kernel.db.tracked.find_one()
        assert '_random' in obj
        assert obj['doc_class'] == 'default'
        assert obj['metadata'] == {'pi': 3}
        assert obj['versions'] == []

        # track same url again with same metadata returns id
        id2 = self.kernel.track_url('http://example.com', 'default', pi=3)
        assert id1 == id2

        # test manually set id
        out = self.kernel.track_url('http://example.com/2', 'default',
                                    'fixed-id')
        assert out == 'fixed-id'

        # can't pass track same id twice with different url
        assert_raises(ValueError, self.kernel.track_url,
                      'http://example.com/3', 'default', 'fixed-id')

        # ... or different doc class
        assert_raises(ValueError, self.kernel.track_url,
                      'http://example.com/2', 'change-hook', 'fixed-id')

        # different metadata is ok, but it should be updated
        self.kernel.track_url('http://example.com/2', 'default', 'fixed-id',
                              pi=3)
        self.kernel.db.tracked.find_one({'_id': 'fixed-id'})['metadata']['pi'] == 3

    def test_no_update(self):
        # update
        self.kernel.track_url('http://example.com', 'one-time')
        obj = self.kernel.db.tracked.find_one()
        self.kernel.update(obj)

        newobj = self.kernel.db.tracked.find_one()
        assert newobj['next_update'] == None

        assert self.kernel.get_update_queue() == []
        assert self.kernel.get_update_queue_size() == 0

    def test_md5_versioning(self):
        assert not self.kernel.md5_versioning('hello!', 'hello!')
        assert self.kernel.md5_versioning('hello!', 'hey!')

    def test_update(self):
        # get a single document tracked and call update on it
        self.kernel.track_url('http://example.com', 'default')
        obj = self.kernel.db.tracked.find_one()
        self.kernel.update(obj)

        # check that the metadata has been updated
        newobj = self.kernel.db.tracked.find_one()
        assert (newobj['last_update'] + datetime.timedelta(minutes=30) ==
                newobj['next_update'])
        first_update = newobj['last_update']
        assert newobj['consecutive_errors'] == 0

        assert len(newobj['versions']) == 1

        # and do another update..
        self.kernel.update(obj)

        # hopefully example.com hasn't changed, this tests that md5 worked
        assert len(newobj['versions']) == 1

        # check that appropriate metadata updated
        newobj = self.kernel.db.tracked.find_one()
        assert first_update < newobj['last_update']

    def test_update_failure(self):
        # track a non-existent URL
        self.kernel.track_url('http://not_a_url', 'default')
        obj = self.kernel.db.tracked.find_one()
        self.kernel.update(obj)

        obj = self.kernel.db.tracked.find_one()
        assert obj['consecutive_errors'] == 1

        # update again
        self.kernel.update(obj)

        obj = self.kernel.db.tracked.find_one()
        assert obj['consecutive_errors'] == 2

    #def test_update_onchanged_fire_only_on_change(self):
    #    self.kernel.track_url('http://example.com', 'change-hook')
    #    obj = self.kernel.db.tracked.find_one()
    #    self.kernel.update(obj)

    #    doc = self.kernel.db.tracked.find_one()
    #    assert doc['hook_fired'] == 1

    #    # again, we rely on example.com not updating
    #    self.kernel.update(obj)
    #    doc = self.kernel.db.tracked.find_one()
    #    assert doc['hook_fired'] == 1

    #def test_update_onchanged_fire_again_on_change(self):
    #    self.kernel.track_url(RANDOM_URL, 'change-hook')
    #    obj = self.kernel.db.tracked.find_one()
    #    self.kernel.update(obj)

    #    doc = self.kernel.db.tracked.find_one()
    #    assert doc['hook_fired'] == 1

    #    # we rely on this URL updating
    #    self.kernel.update(obj)
    #    doc = self.kernel.db.tracked.find_one()
    #    assert doc['hook_fired'] == 2

    def test_get_update_queue(self):
        self.kernel.track_url('never-updates', 'fast-update')
        self.kernel.track_url('bad-uri', 'fast-update')
        self.kernel.track_url('http://example.com', 'fast-update')

        never = self.kernel.db.tracked.find_one(dict(url='never-updates'))
        bad = self.kernel.db.tracked.find_one(dict(url='bad-uri'))
        good = self.kernel.db.tracked.find_one(dict(url='http://example.com'))

        # 3 in queue, ordered by random
        queue = self.kernel.get_update_queue()
        assert len(queue) == 3
        assert queue[0]['_random'] < queue[1]['_random'] < queue[2]['_random']

        # try and update bad & good
        self.kernel.update(bad)
        self.kernel.update(good)

        # queue should only have never in it
        queue = self.kernel.get_update_queue()
        assert queue[0]['_id'] == never['_id']

        # wait for time to pass so queue should be full
        time.sleep(1)
        queue = self.kernel.get_update_queue()
        assert len(queue) == 3

    def test_get_update_queue_size(self):
        self.kernel.track_url('a', 'fast-update')
        self.kernel.track_url('b', 'fast-update')
        self.kernel.track_url('c', 'fast-update')

        a = self.kernel.db.tracked.find_one(dict(url='a'))

        # size should start at 3
        assert self.kernel.get_update_queue_size() == 3

        # goes down one
        self.kernel.update(a)
        assert self.kernel.get_update_queue_size() == 2

        # wait for it to go back to 3
        time.sleep(1)
        assert self.kernel.get_update_queue_size() == 3
