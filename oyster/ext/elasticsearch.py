from celery.task.base import Task

from ..core import kernel
from ..conf import settings

from pyes import ES

es = ES(settings.ELASTICSEARCH_HOST)

class ElasticSearchPush(Task):
    # results go straight to elasticsearch
    ignore_result = True
    action = 'elasticsearch'

    def run(self, doc_id):
        doc = kernel.db.tracked.find_one({'_id': doc_id})

        try:
            text = kernel.extract_text(doc)

            kernel.log(self.action, doc_id, error=False)

            es.index(dict(doc['metadata'], text=text),
                     settings.ELASTICSEARCH_INDEX,
                     settings.ELASTICSEARCH_DOC_TYPE,
                     id=doc_id)
        except Exception as e:
            kernel.log(self.action, doc_id, error=True, exception=str(e))
