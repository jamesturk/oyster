import multiprocessing
import threading
import time
import urllib

import flask

from oyster.client import Client
from oyster.web import app


class UpdateProcess(multiprocessing.Process):

    def __init__(self, task_q):
        super(UpdateProcess, self).__init__()
        self.task_q = task_q
        self.client = Client()


    def run(self):
        while True:
            task = self.task_q.get()

            # break on 'None' poison pill
            if task is None:
                self.task_q.task_done()
                break

            # update tracked document
            self.client.update(task)

            # decrement count for semaphore
            self.task_q.task_done()


def flask_process():
    app.run(debug=True)


def main():
    num_processes = 4
    debug = True

    work_queue = multiprocessing.JoinableQueue()
    workers = [UpdateProcess(work_queue) for i in xrange(num_processes)]
    server = multiprocessing.Process(target=flask_process)

    # give flask access to our work_queue
    app.work_queue = work_queue

    for worker in workers:
        worker.start()
    server.start()

    client = Client()

    while True:
        # get everything overdue and put it into the queue
        next_set = client.get_update_queue()
        if next_set:
            for item in next_set:
                work_queue.put(item)

            # do all queued work
            work_queue.join()

        else:
            # allow for a quiet period if queue is exhausted
            time.sleep(60)

if __name__ == '__main__':
    main()
