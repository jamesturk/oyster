from nose.plugins.skip import SkipTest

from oyster.conf import settings
from oyster.core import Kernel
from oyster.storage.gridfs import GridFSStorage
from oyster.storage.dummy import DummyStorage


def _simple_storage_test(StorageCls):
    kernel = Kernel(mongo_db='oyster_test')
    kernel.doc_classes['default'] = {}
    storage = StorageCls(kernel)

    # ensure the class has a storage_type attribute
    assert hasattr(storage, 'storage_type')

    doc = {'_id': 'aabbccddeeff', 'url': 'http://localhost:8000/#test',
           'doc_class': 'default', 'metadata': {} }
    storage_id = storage.put(doc, 'hello oyster', 'text/plain')
    assert storage_id

    assert storage.get(storage_id) == 'hello oyster'


def test_s3():
    if not hasattr(settings, 'AWS_BUCKET'):
        raise SkipTest('S3 not configured')
    from oyster.storage.s3 import S3Storage
    _simple_storage_test(S3Storage)


def test_gridfs():
    _simple_storage_test(GridFSStorage)


def test_dummy():
    _simple_storage_test(DummyStorage)
