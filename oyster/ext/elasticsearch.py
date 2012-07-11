import logging
from celery.task.base import Task

from ..core import kernel
from ..conf import settings

from pyes import ES

es = ES(settings.ELASTICSEARCH_HOST)
log = logging.getLogger('oyster.ext.elasticsearch')

class ElasticSearchPush(Task):
    # results go straight to elasticsearch
    ignore_result = True

    def run(self, doc_id):
        doc = kernel.db.tracked.find_one({'_id': doc_id})

        try:
            text = kernel.extract_text(doc)
            if not text:
                log.info('no text for %s', doc_id,
                         extra={'doc_class':doc['doc_class']})
                return

            log.info('tracked %s', doc_id,
                     extra={'doc_class':doc['doc_class']})

            es.index(dict(doc['metadata'], text=text),
                     settings.ELASTICSEARCH_INDEX,
                     settings.ELASTICSEARCH_DOC_TYPE,
                     id=doc_id)
        except Exception as e:
            log.warning('error tracking %s', doc_id,
                        extra={'doc_class':doc['doc_class']}, exc_info=True)
            raise
