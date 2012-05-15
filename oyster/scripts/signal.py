#!/usr/bin/env python
import argparse
import traceback
import random
from celery.execute import send_task
from celery import current_app

from oyster.core import kernel

def main():
    parser = argparse.ArgumentParser(
        description='do a task for all documents in a doc_class',
    )

    parser.add_argument('task', type=str, help='task name to apply')
    parser.add_argument('doc_class', type=str,
                        help='doc_class to apply function to')
    parser.add_argument('--sample', action='store_true')
    parser.add_argument('--immediate', action='store_true')

    args = parser.parse_args()

    docs = kernel.db.tracked.find({'doc_class': args.doc_class,
                                   'versions': {'$ne': []}
                                  }, timeout=False)
    total = docs.count()
    print '{0} docs in {1}'.format(total, args.doc_class)

    if args.sample:
        limit = 100
        print 'sampling {0} documents'.format(limit)
        docs = docs[limit(limit).skip(random.randint(0, total-limit))
        args.immediate = True

    errors = 0

    if args.immediate:
        module, name = args.task.rsplit('.', 1)
        task = getattr(__import__(module, fromlist=[name]), name)
        for doc in docs:
            try:
                task.apply((doc['_id'],), throw=True)
            except Exception:
                errors += 1
                traceback.print_exc()
        print '{0} errors in {1} documents'.format(errors, total)

    else:
        for doc in docs:
            send_task(args.task, (doc['_id'], ))

if __name__ == '__main__':
    main()
