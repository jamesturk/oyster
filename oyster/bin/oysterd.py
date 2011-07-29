import multiprocessing
import signal
import threading
import time
import urllib

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


class Producer(threading.Thread):
    def __init__(self, queue, sleep_length):
        super(Producer, self).__init__()
        self.queue = queue
        self.sleep_length = sleep_length
        self.client = Client()
        self._stop = threading.Event()

    def run(self):
        # go forever
        while not self.stopped():

            # get everything overdue and put it into the queue
            next_set = self.client.get_update_queue()
            if next_set:
                for item in next_set:
                    self.queue.put(item)

                # do all queued work
                self.queue.join()

            else:
                # allow for a quiet period if queue is exhausted
                time.sleep(self.sleep_length)

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.is_set()


def main_process():
    num_processes = 4

    work_queue = multiprocessing.JoinableQueue()
    producer = Producer(work_queue, 60)
    workers = [UpdateProcess(work_queue) for i in xrange(num_processes)]
    for w in workers:
        w.start()
    producer.start()


def flask_process():
    app.run(debug=True)


def main():
    num_processes = 4
    debug = True

    work_queue = multiprocessing.JoinableQueue()
    producer = Producer(work_queue, 60)
    workers = [UpdateProcess(work_queue) for i in xrange(num_processes)]
    server = multiprocessing.Process(target=flask_process)

    def cleanup(signal, frame):
        for worker in workers:
            worker.terminate()
        producer.stop()
        server.terminate()

    for worker in workers:
        worker.start()
    producer.start()
    server.start()

if __name__ == '__main__':
    main()
