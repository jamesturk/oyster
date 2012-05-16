"""
    MongoDB handler for Python Logging

    inspired by https://github.com/andreisavu/mongodb-log
"""

import logging
import datetime
import socket
import pymongo


class MongoFormatter(logging.Formatter):

    def format(self, record):
        """ turn a LogRecord into something mongo can store """
        data = record.__dict__.copy()

        data.update(
            # format message
            message=record.getMessage(),
            # overwrite created (float) w/ a mongo-compatible datetime
            created=datetime.datetime.utcnow(),
            host=socket.gethostname(),
            args=tuple(unicode(arg) for arg in record.args)
        )
        data.pop('msecs')   # not needed, stored in created

        # TODO: ensure everything in 'extra' is MongoDB-ready
        exc_info = data.get('exc_info')
        if exc_info:
            data['exc_info'] = self.formatException(exc_info)
        return data


class MongoHandler(logging.Handler):
    def __init__(self, db, collection='logs', host='localhost', port=None,
                 capped_size=100000000, level=logging.NOTSET, async=True):
        db = pymongo.connection.Connection(host, port)[db]
        # try and create the capped log collection
        if capped_size:
            try:
                db.create_collection(collection, capped=True, size=capped_size)
            except pymongo.errors.CollectionInvalid:
                pass
        self.collection = db[collection]
        self.async = async
        logging.Handler.__init__(self, level)
        self.formatter = MongoFormatter()

    def emit(self, record):
        # explicitly set safe=False to get async insert
        # TODO: what to do if an error occurs? not safe to log-- ignore?
        self.collection.save(self.format(record), safe=not self.async)
