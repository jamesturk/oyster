import urllib
import boto
from oyster.conf import settings


class DummyStorage(object):
    """ should NOT be used outside of testing """

    storage_type = 'dummy'

    def __init__(self, kernel):
        self._storage = {}

    def put(self, tracked_doc, data, content_type):
        """ store the document in local dict """
        self._storage[tracked_doc['_id']] = data
        return tracked_doc['_id']

    def get(self, id):
        return self._storage[id]
