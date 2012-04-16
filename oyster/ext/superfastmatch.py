from celery.task.base import Task

from ..core import kernel
from ..conf import settings

from superfastmatch import Client

sfm = Client(settings.SUPERFASTMATCH_URL)


class SuperFastMatchPush(Task):
    """ task that pushes documents to SFM """

    # results go straight to database
    ignore_result = True

    def run(self, doc_id):
        doc = kernel.db.tracked.find_one({'_id': doc_id})
        text = kernel.extract_text(doc)
        doctype, docid = settings.SUPERFASTMATCH_ID_FUNC(doc_id)
        sfm.add(doctype, docid, text, **doc['metadata'])
