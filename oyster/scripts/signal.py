#!/usr/bin/env python
import argparse
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

    args = parser.parse_args()

    docs = kernel.db.tracked.find({'doc_class': args.doc_class,
                                   'versions': {'$ne': []}
                                  }, timeout=False)
    print '%s docs in %s' % (docs.count(), args.doc_class)

    if args.sample:
        print 'sampling 100 documents'
        docs = docs.limit(100)
        task = current_app.tasks[name]
        for doc in docs:
            task.apply((doc['_id'],))

    else:
        for doc in docs:
            send_task(args.task, (doc['_id'], ))

if __name__ == '__main__':
    main()
