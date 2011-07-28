import datetime
import hashlib
import random
import sys

import pymongo
import gridfs


class Client(object):

    def __init__(self, host='localhost', port=27017,
                 database='oyster', collection='fs'):
        self.db = pymongo.Connection(host, port)[database]
        self.fs = gridfs.GridFS(self.db, collection)


    def _wipe(self):
        self.db.drop_collection('tracked')
        self.db.fs.chunks.drop()
        self.db.fs.files.drop()


    def track_url(self, url, versioning='md5', update_mins=60*24, **kwargs):
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


    def add_version(self, doc, data):
        """
        Add a version
        """

        metadata = doc['metadata']
        do_put = True

        # add if md5 has changed
        if doc['versioning'] == 'md5':
            try:
                old_md5 = self.fs.get_last_version(filename=doc['url']).md5
                new_md5 = hashlib.md5(data).hexdigest()
                do_put = (old_md5 != new_md5)
            except gridfs.NoFile:
                pass

        else:
            raise ValueError('unknown versioning strategy "%s"' %
                             doc['versioning'])

        doc['_last_update'] = datetime.datetime.utcnow()
        doc['_next_update'] = (doc['_last_update'] +
                               datetime.timedelta(minutes=doc['update_mins']))
        self.db.tracked.save(doc, safe=True)

        # true unless md5s were the same
        if do_put:
            self.fs.put(data, filename=doc['url'], **metadata)


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
