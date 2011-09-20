import datetime
import hashlib
import random
import sys
import urllib

import pymongo
import gridfs
import scrapelib


def get_configured_client():
    """ helper factory, gets a client configured with oyster.conf.settings """
    from oyster.conf import settings
    return Client(mongo_host=settings.MONGO_HOST,
                  mongo_port=settings.MONGO_PORT,
                  mongo_db=settings.MONGO_DATABASE,
                  mongo_log_maxsize=settings.MONGO_LOG_MAXSIZE,
                  user_agent=settings.USER_AGENT,
                  rpm=settings.REQUESTS_PER_MINUTE,
                  timeout=settings.REQUEST_TIMEOUT,
                  retry_attempts=settings.RETRY_ATTEMPTS,
                  retry_wait_seconds=settings.RETRY_WAIT_SECONDS)


class Client(object):
    """ oyster's workhorse, handles tracking """

    def __init__(self, mongo_host='localhost', mongo_port=27017,
                 mongo_db='oyster', mongo_log_maxsize=100000000,
                 user_agent='oyster', rpm=600, timeout=None,
                 retry_attempts=0, retry_wait_seconds=5):

        # set up a capped log if it doesn't exist
        self.db = pymongo.Connection(mongo_host, mongo_port)[mongo_db]
        try:
            self.db.create_collection('logs', capped=True,
                                      size=mongo_log_maxsize)
        except pymongo.errors.CollectionInvalid:
            pass

        # create status document if it doesn't exist
        if self.db.status.count() == 0:
            self.db.status.insert({'update_queue': 0})

        self._collection_name = 'fs'
        self.fs = gridfs.GridFS(self.db, self._collection_name)
        self.scraper = scrapelib.Scraper(user_agent=user_agent,
                                         requests_per_minute=rpm,
                                         follow_robots=False,
                                         raise_errors=True,
                                         timeout=timeout,
                                         retry_attempts=retry_attempts,
                                         retry_wait_seconds=retry_wait_seconds
                                        )


    def _wipe(self):
        """ exists primarily for debug use, wipes entire db """
        self.db.drop_collection('tracked')
        self.db.drop_collection('%s.chunks' % self._collection_name)
        self.db.drop_collection('%s.files' % self._collection_name)
        self.db.drop_collection('logs')
        self.db.drop_collection('status')


    def log(self, action, url, error=False, **kwargs):
        """ add an entry to the oyster log """
        kwargs['action'] = action
        kwargs['url'] = url
        kwargs['error'] = error
        kwargs['timestamp'] = datetime.datetime.utcnow()
        self.db.logs.insert(kwargs)


    def track_url(self, url, versioning='md5', update_mins=60*24,
                  **kwargs):
        """
        Add a URL to the set of tracked URLs, accessible via a given filename.

        url
            URL to start tracking
        """
        if self.db.tracked.find_one({'url': url}):
            self.log('track', url=url, error='already tracked')
            raise ValueError('%s is already tracked' % url)

        self.log('track', url=url)
        self.db.tracked.insert(dict(url=url, versioning=versioning,
                                    update_mins=update_mins,
                                    _random=random.randint(0, sys.maxint),
                                    metadata=kwargs))


    def md5_versioning(self, doc, data):
        """ return True if md5 changed or if file is new """
        try:
            old_md5 = self.fs.get_last_version(filename=doc['url']).md5
            new_md5 = hashlib.md5(data).hexdigest()
            return (old_md5 != new_md5)
        except gridfs.NoFile:
            return True


    def update(self, doc):
        do_put = True
        error = False

        # update strategies could be implemented here as well
        try:
            url = doc['url'].replace(' ', '%20')
            data = self.scraper.urlopen(url)
            content_type = data.response.headers['content-type']
        except Exception as e:
            do_put = False
            error = str(e)

        # versioning is a concept for future use, but here's how it can work:
        #  versioning functions take doc & data, and return True if data is
        #  different, since they have access to doc, they can also modify
        #  certain attributes as needed

        if do_put:
            if doc['versioning'] == 'md5':
                do_put = self.md5_versioning(doc, data)
            else:
                raise ValueError('unknown versioning strategy "%s"' %
                                 doc['versioning'])

        if do_put:
            self.fs.put(data, filename=doc['url'], content_type=content_type,
                        **doc['metadata'])

        # last_update/next_update are separate from question of versioning
        doc['last_update'] = datetime.datetime.utcnow()
        doc['next_update'] = (doc['last_update'] +
                              datetime.timedelta(minutes=doc['update_mins']))
        if error:
            doc['consecutive_errors'] = doc.get('consecutive_errors', 0) + 1
        else:
            doc['consecutive_errors'] = 0

        self.log('update', url=url, new_doc=do_put, error=error)

        self.db.tracked.save(doc, safe=True)


    def get_all_versions(self, url):
        versions = []
        n = 0
        while True:
            try:
                versions.append(self.fs.get_version(url, n))
                n += 1
            except gridfs.NoFile:
                break
        return versions


    def get_version(self, url, n=-1):
        return self.fs.get_version(url, n)


    def get_update_queue(self):
        # results are always sorted by random to avoid piling on single server

        # first we try to update anything that we've never retrieved
        new = self.db.tracked.find({'next_update':
                                    {'$exists': False}}).sort('_random')
        queue = list(new)

        # pull the rest from those for which next_update is in the past
        next = self.db.tracked.find({'next_update':
             {'$lt': datetime.datetime.utcnow()}}).sort('_random')
        queue.extend(next)

        return queue


    def get_update_queue_size(self):
        new = self.db.tracked.find({'next_update': {'$exists': False}}).count()
        next = self.db.tracked.find({'next_update':
                                 {'$lt': datetime.datetime.utcnow()}}).count()
        return new+next
