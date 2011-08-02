import argparse
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




def main():

    parser = argparse.ArgumentParser(description='oyster daemon')
    parser.add_argument('-w', '--workers', type=int, default=0,
        help='number of worker processes to use (default: # processors)')
    parser.add_argument('-p', '--port', type=int, default=31687,
        help='port for HTTP service to run on')
    parser.add_argument('--debug', action='store_true',
        help='enable debug mode')
    # client config options
    parser.add_argument('--mongo_host', default='localhost',
        help='host or IP for mongodb server')
    parser.add_argument('--mongo_port', default=27017,
        help='port for mongodb server')
    parser.add_argument('--mongo_db', default='oyster',
        help='mongodb database name')
    parser.add_argument('--logsize', default=100000000,
        help='mongodb maximum log size (bytes)')
    parser.add_argument('--useragent', default='oyster',
        help='user agent to use when fetching pages')
    parser.add_argument('--rpm', default=600,
        help='maximum requests per minute to make')
    parser.add_argument('--timeout', default=None,
        help='timeout (seconds) when making requests (default: none)')
    parser.add_argument('--retry_attempts', default=0,
        help='retries when making requests (default: 0)')
    parser.add_argument('--retry_wait', default=5,
        help='retry wait period (seconds) when making requests (default: 5)')
    args = parser.parse_args()

    work_queue = multiprocessing.JoinableQueue()

    # workers defaults to cpu_count
    if not args.workers:
        args.workers = multiprocessing.cpu_count()
    workers = [UpdateProcess(work_queue) for i in xrange(args.workers)]

    # separate process for Flask app
    def flask_process():
        app.run(debug=args.debug, port=args.port)
    server = multiprocessing.Process(target=flask_process)

    # give flask access to our work_queue
    app.work_queue = work_queue

    # start all processes
    for worker in workers:
        worker.start()
    server.start()

    client = Client(mongo_host=args.mongo_host,
                    mongo_port=args.mongo_port,
                    mongo_db=args.mongo_db,
                    mongo_log_maxsize=args.logsize,
                    user_agent=args.useragent,
                    rpm=args.rpm,
                    timeout=args.timeout,
                    retry_attempts=args.retry_attempts,
                    retry_wait_seconds=args.retry_wait_seconds)

    while True:
        # get everything overdue and put it into the queue
        next_set = client.get_update_queue()
        if next_set:
            for item in next_set:
                work_queue.put(item)

            # do all queued work
            work_queue.join()

        # sleep for a minute between attempts to check the queue
        time.sleep(60)


if __name__ == '__main__':
    main()
