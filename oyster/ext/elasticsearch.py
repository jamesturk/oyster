from celery.task.base import Task

from ..core import kernel
from ..conf import settings

from pyes import ES

es = ES(settings.ELASTICSEARCH_HOST)

class ElasticSearchPush(Task):
    # results go straight to elasticsearch
    ignore_result = True

    def run(self, doc_id):
        doc = kernel.db.tracked.find_one({'_id': doc_id})
        text = kernel.extract_text(doc)

        self.get_logger().debug('adding {1} to ElasticSearch'.format(doc_id))

        for i, piece in enumerate(pieces):
            es.index(dict(doc['metadata'], text=text),
                     settings.ELASTICSEARCH_INDEX,
                     settings.ELASTICSEARCH_DOC_TYPE,
                     id=doc_id)
