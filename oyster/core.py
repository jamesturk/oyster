import datetime
import logging
import hashlib
import random
import sys

import pymongo
import scrapelib

from .mongolog import MongoHandler
from .storage import engines
from celery.execute import send_task


class Kernel(object):
    """ oyster's workhorse, handles tracking """

    def __init__(self, mongo_host='localhost', mongo_port=27017,
                 mongo_db='oyster', mongo_log_maxsize=100000000,
                 user_agent='oyster', rpm=60, timeout=300,
                 retry_attempts=3, retry_wait_minutes=60,
                 doc_classes=None, default_storage_engine='dummy',
                ):
        """
        configurable for ease of testing, only one should be instantiated
        """

        # set up the log
        self.db = pymongo.Connection(mongo_host, mongo_port)[mongo_db]

        self.log = logging.getLogger('oyster')
        self.log.setLevel(logging.DEBUG)
        self.log.addHandler(MongoHandler(mongo_db, host=mongo_host,
                                         port=mongo_port,
                                         capped_size=mongo_log_maxsize))

        # create status document if it doesn't exist
        if self.db.status.count() == 0:
            self.db.status.insert({'update_queue': 0})

        # ensure an index on _random
        self.db.tracked.ensure_index('_random')
        self.db.tracked.ensure_index('url')

        self.scraper = scrapelib.Scraper(user_agent=user_agent,
                                         requests_per_minute=rpm,
                                         follow_robots=False,
                                         raise_errors=True,
                                         timeout=timeout)

        self.retry_attempts = retry_attempts
        self.retry_wait_minutes = retry_wait_minutes

        # load engines
        self.storage = {}
        for name, StorageCls in engines.iteritems():
            self.storage[name] = StorageCls(self)

        # set document classes
        _doc_class_fields = ('update_mins', 'onchanged')
        self.doc_classes = doc_classes or {}
        for dc_name, dc_props in self.doc_classes.iteritems():
            for key in _doc_class_fields:
                if key not in dc_props:
                    raise ValueError('doc_class %s missing key %s' %
                                     (dc_name, key))
            # set a default storage engine
            if 'storage_engine' not in dc_props:
                dc_props['storage_engine'] = default_storage_engine

    def _wipe(self):
        """ exists primarily for debug use, wipes entire db """
        self.db.drop_collection('tracked')
        self.db.drop_collection('logs')
        self.db.drop_collection('status')

    def _add_doc_class(self, doc_class, **properties):
        self.doc_classes[doc_class] = properties

    def track_url(self, url, doc_class, id=None, **kwargs):
        """
        Add a URL to the set of tracked URLs, accessible via a given filename.

        url
            URL to start tracking
        doc_class
            document type, can be any arbitrary string
        **kwargs
            any keyword args will be added to the document's metadata
        """
        if doc_class not in self.doc_classes:
            error = 'error tracking %s, unregistered doc_class %s'
            self.log.error(error, url, doc_class)
            raise ValueError(error % (url, doc_class))

        # try and find an existing version of this document
        tracked = None

        if id:
            tracked = self.db.tracked.find_one({'_id': id})
        else:
            tracked = self.db.tracked.find_one({'url': url})

        # if id exists, ensure that URL and doc_class haven't changed
        # then return existing data (possibly with refreshed metadata)
        if tracked:
            if (tracked['url'] == url and
                tracked['doc_class'] == doc_class):
                if kwargs != tracked['metadata']:
                    tracked['metadata'] = kwargs
                    self.db.tracked.save(tracked, safe=True)
                return tracked['_id']
            else:
                # id existed but with different URL
                message = ('%s already exists with different data (tracked: '
                           '%s, %s) (new: %s, %s)')
                args = (tracked['_id'], tracked['url'], tracked['doc_class'],
                        url, doc_class)
                self.log.error(message, *args)
                raise ValueError(message % args)

        self.log.info('tracked %s [%s]', url, id)

        newdoc = dict(url=url, doc_class=doc_class,
                      _random=random.randint(0, sys.maxint),
                      versions=[], metadata=kwargs)
        if id:
            newdoc['_id'] = id
        return self.db.tracked.insert(newdoc, safe=True)

    def md5_versioning(self, olddata, newdata):
        """ return True if md5 changed or if file is new """
        old_md5 = hashlib.md5(olddata).hexdigest()
        new_md5 = hashlib.md5(newdata).hexdigest()
        return old_md5 != new_md5

    def update(self, doc):
        """
        perform update upon a given document

        :param:`doc` must be a document from the `tracked` collection

        * download latest document
        * check if document has changed using versioning func
        * if a change has occurred save the file
        * if error occured, log & keep track of how many errors in a row
        * update last_update/next_update timestamp
        """

        new_version = True
        error = False
        now = datetime.datetime.utcnow()

        try:
            doc_class = self.doc_classes[doc['doc_class']]
        except KeyError:
            raise ValueError('unregistered doc_class %s' % doc['doc_class'])

        update_mins = doc_class['update_mins']
        storage = self.storage[doc_class['storage_engine']]

        # fetch strategies could be implemented here as well
        try:
            url = doc['url'].replace(' ', '%20')
            newdata = self.scraper.urlopen(url)
            content_type = newdata.response.headers['content-type']
        except Exception as e:
            new_version = False
            error = str(e)

        # only do versioning check if at least one version exists
        if new_version and doc['versions']:
            # room here for different versioning schemes
            olddata = storage.get(doc['versions'][-1]['storage_key'])
            new_version = self.md5_versioning(olddata, newdata)

        if new_version:
            storage_id = storage.put(doc, newdata, content_type)
            doc['versions'].append({'timestamp': now,
                                    'storage_key': storage_id,
                                    'storage_type': storage.storage_type,
                                   })
            # fire off onchanged functions
            for onchanged in doc_class.get('onchanged', []):
                send_task(onchanged, (doc['_id'],))

        if error:
            # if there's been an error, increment the consecutive_errors count
            # and back off a bit until we've reached our retry limit
            c_errors = doc.get('consecutive_errors', 0)
            doc['consecutive_errors'] = c_errors + 1
            if c_errors <= self.retry_attempts:
                update_mins = self.retry_wait_minutes * (2 ** c_errors)
        else:
            # reset error count if all was ok
            doc['consecutive_errors'] = 0

        # last_update/next_update are separate from question of versioning
        doc['last_update'] = now
        if update_mins:
            doc['next_update'] = now + datetime.timedelta(minutes=update_mins)
        else:
            doc['next_update'] = None

        if error:
            self.log.warning('error updating %s [%s]', url, doc['_id'])
        else:
            new_version = ' (new)'
            self.log.info('updated %s [%s]%s', url, doc['_id'], new_version)

        self.db.tracked.save(doc, safe=True)

    def get_update_queue(self):
        """
        Get a list of what needs to be updated.

        Documents that have never been updated take priority, followed by
        documents that are simply stale.  Within these two categories results
        are sorted in semirandom order to decrease odds of piling on one
        server.
        """
        # results are always sorted by random to avoid piling on single server

        # first we try to update anything that we've never retrieved
        new = self.db.tracked.find({'next_update':
                                    {'$exists': False}}).sort('_random')
        queue = list(new)

        # pull the rest from those for which next_update is in the past
        next = self.db.tracked.find({'$and': [
            {'next_update': {'$ne': None}},
            {'next_update': {'$lt': datetime.datetime.utcnow()}},
        ]}).sort('_random')
        queue.extend(next)

        return queue

    def get_update_queue_size(self):
        """
        Get the size of the update queue, this should match
        ``len(self.get_update_queue())``, but is computed more efficiently.
        """
        new = self.db.tracked.find({'next_update': {'$exists': False}}).count()
        next = self.db.tracked.find({'$and': [
            {'next_update': {'$ne': None}},
            {'next_update': {'$lt': datetime.datetime.utcnow()}},
        ]}).count()
        return new + next

    def get_last_version(self, doc):
        try:
            doc_class = self.doc_classes[doc['doc_class']]
        except KeyError:
            raise ValueError('unregistered doc_class %s' % doc['doc_class'])
        storage = self.storage[doc_class['storage_engine']]
        return storage.get(doc['versions'][-1]['storage_key'])

    def extract_text(self, doc):
        version = self.get_last_version(doc)
        doc_class = self.doc_classes[doc['doc_class']]
        try:
            extract_text = doc_class['extract_text']
        except KeyError:
            raise ValueError('doc_class %s missing extract_text' %
                             doc['doc_class'])
        return extract_text(doc, version)


def _get_configured_kernel():
    """ factory, gets a connection configured with oyster.conf.settings """
    from oyster.conf import settings
    return Kernel(mongo_host=settings.MONGO_HOST,
                  mongo_port=settings.MONGO_PORT,
                  mongo_db=settings.MONGO_DATABASE,
                  mongo_log_maxsize=settings.MONGO_LOG_MAXSIZE,
                  user_agent=settings.USER_AGENT,
                  rpm=settings.REQUESTS_PER_MINUTE,
                  timeout=settings.REQUEST_TIMEOUT,
                  retry_attempts=settings.RETRY_ATTEMPTS,
                  retry_wait_minutes=settings.RETRY_WAIT_MINUTES,
                  doc_classes=settings.DOCUMENT_CLASSES,
                  default_storage_engine=settings.DEFAULT_STORAGE_ENGINE,
                 )

kernel = _get_configured_kernel()
