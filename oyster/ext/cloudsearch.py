# needed so we can import cloudsearch
from __future__ import absolute_import

from celery.task.base import Task

from ..core import kernel
from ..conf import settings

from cloudsearch import CloudSearch

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
