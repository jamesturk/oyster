#!/usr/bin/env python
import argparse

from oyster.core import kernel


def main():
    parser = argparse.ArgumentParser(
        description='do a task for all documents in a doc_class',
    )

    parser.add_argument('function', type=str, help='path to function to apply')
    parser.add_argument('doc_class', type=str,
                        help='doc_class to apply function to')

    args = parser.parse_args()

    docs = kernel.db.tracked.find({'doc_class': args.doc_class})
    print '%s docs in %s' % (docs.count(), args.doc_class)

    path, func = args.function.rsplit('.', 1)
    mod = __import__(path, fromlist=[func])
    func = getattr(mod, func)

    for doc in docs:
        func(doc, kernel.get_last_version(doc))
        # optionally save doc?

if __name__ == '__main__':
    main()
