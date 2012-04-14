import json
import requests
from celery.task.base import Task

from ..core import kernel
from ..conf import settings


class CloudSearch(object):

    # slightly under 5MB
    MAX_BYTES = 5240000

    def __init__(self, name, domainid, flush_every=None):
        self.doc_url = 'http://doc-{0}-{1}.us-east-1.cloudsearch.amazonaws.com/2011-02-01/documents/batch'.format(name, domainid)
        self.search_url = 'http://search-{0}-{1}.us-east-1.cloudsearch.amazonaws.com/2011-02-01/search'.format(name, domainid)

        self._current_batch = []
        self._current_size = 0
        self._flush_every = flush_every


    def flush(self):
        print 'flushing!'
        if self._current_batch:
            payload = '[{0}]'.format(','.join(self._current_batch))
            resp = requests.post(self.doc_url, payload,
                                 headers={'content-type': 'application/json'})
            self._current_batch = []
            self._current_size = 0
            if resp.status_code >= 400:
                # http://docs.amazonwebservices.com/cloudsearch/latest/developerguide/DocumentsBatch.html
                raise Exception('{0}: {1}'.format(resp.status_code, resp.text))

    def add_document(self, id, **kwargs):
        newdoc = {'type': 'add', 'version': 1, 'lang': 'en',
                  'id': id, 'fields': kwargs}
        newjson = json.dumps(newdoc)
        newsize = len(newjson)

        if (self._current_size + newsize > self.MAX_BYTES or
            (self._flush_every and
             len(self._current_batch) > self._flush_every)):
            self.flush()

        self._current_batch.append(json.dumps(newdoc))
        self._current_size += newsize

    def search_by_expr(self, q, bq=None, size=10, start=0):
        " http://docs.amazonwebservices.com/cloudsearch/latest/developerguide/SearchAPI.html "
        params = {'q': q, 'size': size, 'start': start}
        if bq:
            params['bq'] = bq
        return requests.get(self.search_url, params=params).text

cs = CloudSearch(settings.CLOUDSEARCH_DOMAIN, settings.CLOUDSEARCH_ID, 20)

class CloudSearchPush(Task):
    """ task that updates documents """
    # results go straight to database
    ignore_result = True

    # a bit under 1MB
    MAX_BYTES = 1048000

    def run(self, doc_id):
        doc = kernel.db.tracked.find_one({'_id': doc_id})
        text = kernel.extract_text(doc)
        pieces = [text[i:i+self.MAX_BYTES] for i in
                  xrange(0, len(text), self.MAX_BYTES)]

        self.get_logger().debug('adding {0} pieces for {1}'.format(
            len(pieces), doc_id))
        for i, piece in enumerate(pieces):
            cloud_id = '%s_%s' % (doc_id.lower(), i)
            cs.add_document(cloud_id, text=piece, **doc['metadata'])
