import unittest
import logging
import datetime

import pymongo
from ..mongolog import MongoHandler

class TestMongoLog(unittest.TestCase):

    DB_NAME = 'oyster_test'

    def setUp(self):
        pymongo.Connection().drop_database(self.DB_NAME)
        self.log = logging.getLogger('mongotest')
        self.log.setLevel(logging.DEBUG)
        self.logs = pymongo.Connection()[self.DB_NAME]['logs']
        # clear handlers upon each setup 
        self.log.handlers = []
        # async = False for testing
        self.log.addHandler(MongoHandler(self.DB_NAME, capped_size=4000,
                                         async=False))

    def tearDown(self):
        pymongo.Connection().drop_database(self.DB_NAME)

    def test_basic_write(self):
        self.log.debug('test')
        self.assertEqual(self.logs.count(), 1)
        self.log.debug('test')
        self.assertEqual(self.logs.count(), 2)
        # capped_size will limit these
        self.log.debug('test'*200)
        self.log.debug('test'*200)
        self.assertEqual(self.logs.count(), 1)

    def test_attributes(self):
        self.log.debug('pi=%s', 3.14, extra={'pie':'pizza'})
        logged = self.logs.find_one()
        self.assertEqual(logged['message'], 'pi=3.14')
        self.assertTrue(isinstance(logged['created'], datetime.datetime))
        self.assertTrue('host' in logged)
        self.assertEqual(logged['name'], 'mongotest')
        self.assertEqual(logged['levelname'], 'DEBUG')
        self.assertEqual(logged['pie'], 'pizza')

        # and exc_info
        try:
            raise Exception('error!')
        except:
            self.log.warning('oh no!', exc_info=True)
            logged = self.logs.find_one(sort=[('$natural', -1)])
        self.assertEqual(logged['levelname'], 'WARNING')
        self.assertTrue('error!' in logged['exc_info'])
