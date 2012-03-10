from __future__ import absolute_import

import gridfs


class GridFSStorage(object):
    storage_type = 'gridfs'

    def __init__(self, kernel):
        self.db = kernel.db
        self._collection_name = 'fs'
        self.fs = gridfs.GridFS(self.db, self._collection_name)

    def put(self, tracked_doc, data, content_type):
        return self.fs.put(data, filename=tracked_doc['url'],
                           content_type=content_type,
                           **tracked_doc['metadata'])

    def get(self, id):
        return self.fs.get(id).read()
