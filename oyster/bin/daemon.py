import multiprocessing
import time
import urllib

from ..client import Client

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


def main():
    num_processes = 4
    sleep_upon_exhaustion = 60

    c = Client()
    work_queue = multiprocessing.JoinableQueue()
    workers = [UpdateProcess(work_queue) for i in xrange(num_processes)]
    for w in workers:
        w.start()

    # go forever
    while True:

        # get everything overdue and put it into the queue
        next_set = c.get_update_queue()
        if next_set:
            for item in next_set:
                work_queue.put(item)

            # do all queued work
            work_queue.join()

        else:
            # allow for a quiet period if queue is exhausted
            time.sleep(sleep_upon_exhaustion)


if __name__ == '__main__':
    main()
