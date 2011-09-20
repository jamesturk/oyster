from celery.task.base import Task, PeriodicTask
from celery.execute import send_task

from oyster.client import get_configured_client


class UpdateTask(Task):
    """ task that updates documents """
    # results go straight to database
    ignore_result = True

    def __init__(self):
        # one client per process
        self.client = get_configured_client()


    def run(self, doc_id):
        doc = self.client.db.tracked.find_one({'_id': doc_id})
        self.client.update(doc)
        for hook in doc.get('post_update_hooks', []):
            send_task(hook, (doc_id,))
        self.client.db.status.update({}, {'$inc': {'update_queue': -1}})


class UpdateTaskScheduler(PeriodicTask):
    """ recurring task that populates the update queue """

    # 60s tick
    run_every = 60

    def __init__(self):
        self.client = get_configured_client()


    def run(self):
        # if the update queue isn't empty, wait to add more
        # (currently the only way we avoid duplicates)
        # alternate option would be to set a _queued flag on documents
        if self.client.db.status.find_one()['update_queue']:
            return

        next_set = self.client.get_update_queue()
        for doc in next_set:
            update_task.delay(doc['_id'])
            self.client.db.status.update({}, {'$inc': {'update_queue': 1}})
