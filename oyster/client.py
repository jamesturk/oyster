import datetime
import hashlib
import random
import sys

import pymongo
import gridfs
import scrapelib


class Client(object):


    def __init__(self, mongo_host='localhost', mongo_port=27017,
                 mongo_db='oyster', gridfs_collection='fs',
                 user_agent='oyster', rpm=600, follow_robots=False,
                 raise_errors=True, timeout=None, retry_attempts=0,
                 retry_wait_seconds=5):
        self.db = pymongo.Connection(mongo_host, mongo_port)[mongo_db]
        self.fs = gridfs.GridFS(self.db, gridfs_collection)
        self._collection_name = gridfs_collection
        self.scraper = scrapelib.Scraper(user_agent=user_agent,
                                         requests_per_minute=rpm,
                                         follow_robots=False,
                                         raise_errors=True,
                                         timeout=None,
                                         retry_attempts=0,
                                         retry_wait_seconds=5
                                        )


    def _wipe(self):
        """ exists primarily for debug use, wipes entire db """
        self.db.drop_collection('tracked')
        self.db.drop_collection('%s.chunks' % self._collection_name)
        self.db.drop_collection('%s.files' % self._collection_name)


    def track_url(self, url, versioning='md5', update_mins=60*24,
                  **kwargs):
        """
        Add a URL to the set of tracked URLs, accessible via a given filename.

        url
            URL to start tracking
        """
        if self.db.tracked.find_one({'url': url}):
            raise ValueError('%s is already tracked' % url)

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
        # assume we're going to do the put
        do_put = True

        # update strategies could be implemented here as well
        data = self.scraper.urlopen(doc['url'])

        # versioning is a concept for future use, but here's how it can work:
        #  versioning functions take doc & data, and return True if data is
        #  different, since they have access to doc, they can also modify
        #  certain attributes as needed
        if doc['versioning'] == 'md5':
            do_put = self.md5_versioning(doc, data)
        else:
            raise ValueError('unknown versioning strategy "%s"' %
                             doc['versioning'])

        if do_put:
            self.fs.put(data, filename=doc['url'], **doc['metadata'])

        # _last_update/_next_update are separate from question of versioning
        doc['_last_update'] = datetime.datetime.utcnow()
        doc['_next_update'] = (doc['_last_update'] +
                               datetime.timedelta(minutes=doc['update_mins']))
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


    def get_update_queue(self, max=0):
        # results are always sorted by random to avoid piling on
        # a single server

        # first we try to update anything that we've never retrieved
        new = self.db.tracked.find({'_next_update':
                                    {'$exists': False}}).sort('_random')
        if max:
            new = new.limit(max)

        queue = list(new)

        # pull the rest from those for which _next_update is in the past
        next = self.db.tracked.find({'_next_update':
             {'$lt': datetime.datetime.utcnow()}}).sort('_random')
        if max:
            max -= len(queue)
            next = next.limit(max)

        queue.extend(next)

        return queue


    def get_update_queue_size(self):
        return len(self.get_update_queue())
