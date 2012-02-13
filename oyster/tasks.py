from celery.task.base import Task, PeriodicTask
from celery.execute import send_task

from pymongo.objectid import ObjectId

from oyster.conf import settings
from oyster.connection import get_configured_connection


class UpdateTask(Task):
    """ task that updates documents """
    # results go straight to database
    ignore_result = True

    def __init__(self):
        # one connection per process
        self.conn = get_configured_connection()


    def run(self, doc_id):
        doc = self.conn.db.tracked.find_one({'_id': doc_id})
        self.conn.update(doc)
        for hook in doc.get('post_update_hooks', []):
            send_task(hook, (doc_id,))
        self.conn.db.status.update({}, {'$inc': {'update_queue': -1}})


class UpdateTaskScheduler(PeriodicTask):
    """ recurring task that populates the update queue """

    # 60s tick
    run_every = 60
    conn = get_configured_connection()

    def run(self):
        # if the update queue isn't empty, wait to add more
        # (currently the only way we avoid duplicates)
        # alternate option would be to set a _queued flag on documents
        if self.conn.db.status.find_one()['update_queue']:
            return

        next_set = self.conn.get_update_queue()
        for doc in next_set:
            UpdateTask.delay(doc['_id'])
            self.conn.db.status.update({}, {'$inc': {'update_queue': 1}})


class ExternalStoreTask(Task):
    """ base class for tasks that push documents to an external store

        when overiding be sure to define
            external_store
                short string describing external store (eg. 's3')
            upload_document(self, doc_id, filedata, metadata)
                function that uploads the document and returns a unique ID
    """

    # results go straight to database
    ignore_result = True
    # used as a base class
    abstract = True

    def __init__(self):
        # one connection per process
        self.conn = get_configured_connection()

    def run(self, doc_id, extract_text=lambda x: x):
        # get the document
        doc = self.conn.db.tracked.find_one({'_id': ObjectId(doc_id)})
        filedata = self.conn.get_version(doc['url']).read()
        text = extract_text(filedata, doc['metadata'])

        # put the document into the data store
        result = self.upload_document(doc_id, text, doc['metadata'])

        doc[self.external_store + '_id'] = result
        self.conn.db.tracked.save(doc, safe=True)


    def upload_document(self, doc_id, filedata, metadata):
        """ abstract method, override on implementations """
        pass


class S3StoreTask(ExternalStoreTask):
    external_store = 's3'

    import boto
    s3conn = boto.connect_s3(settings.AWS_KEY, settings.AWS_SECRET)

    def upload_document(self, doc_id, filedata, metadata):
        """ upload the document to S3 """
        bucket = self.s3conn.create_bucket(settings.AWS_BUCKET)
        k = self.boto.s3.Key(bucket)
        k.key = doc_id
        k.set_contents_from_string(filedata)
        k.set_acl('public-read')

        url = 'http://%s.s3.amazonaws.com/%s' % (settings.AWS_BUCKET, doc_id)
        return url
