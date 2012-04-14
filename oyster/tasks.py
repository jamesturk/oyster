from celery.task.base import Task, PeriodicTask
from celery.execute import send_task

from oyster.core import kernel


class UpdateTask(Task):
    """ task that updates documents """
    # results go straight to database
    ignore_result = True

    def run(self, doc_id):
        doc = kernel.db.tracked.find_one({'_id': doc_id})
        kernel.db.status.update({}, {'$inc': {'update_queue': -1}})
        kernel.update(doc)
        # don't sit on a connection
        kernel.db.connection.end_request()


class UpdateTaskScheduler(PeriodicTask):
    """ recurring task that populates the update queue """

    # 60s tick
    run_every = 60

    def run(self):
        # if the update queue isn't empty, wait to add more
        # (currently the only way we avoid duplicates)
        # alternate option would be to set a _queued flag on documents
        update_queue_size = kernel.db.status.find_one()['update_queue']
        if update_queue_size:
            self.get_logger().debug('waiting, update_queue_size={0}'.format(
                                    update_queue_size))
            return

        next_set = kernel.get_update_queue()
        if next_set:
            self.get_logger().debug('repopulating update_queue')
        else:
            self.get_logger().debug('kernel.update_queue empty')

        for doc in next_set:
            UpdateTask.delay(doc['_id'])
            kernel.db.status.update({}, {'$inc': {'update_queue': 1}})
            # don't sit on a connection
            kernel.db.connection.end_request()
