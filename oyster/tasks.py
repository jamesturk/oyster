from celery.task.base import Task, PeriodicTask
from celery.execute import send_task

from oyster.client import get_configured_client


class UpdateTask(Task):
    # results go straight to database
    ignore_result = True

    def __init__(self):
        # one client per process
        client = get_configured_client()


    def run(self, doc_id):
        # maybe fetch doc instead?
        doc = client.db.tracked.find_one({'_id': doc_id})
        client.update(doc)
        for hook in doc.get('post_update_hooks', []):
            send_task(hook, (doc_id,))
        client.db.status.update({}, {'$inc': {'update_queue': -1}})


class UpdateTaskScheduler(PeriodicTask):

    # 60s tick
    run_every = 60

    def run(self):

        # if the update queue isn't empty, wait to add more
        # (currently the only way we avoid duplicates)
        # alternate option would be to set a _queued flag on documents
        if client.db.status.find_one()['update_queue']:
            return

        next_set = client.get_update_queue()
        for doc in next_set:
            update_task.delay(doc['_id'])
            client.db.status.update({}, {'$inc': {'update_queue': 1}})
