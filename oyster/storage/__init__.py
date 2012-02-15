
engines = {}
try:
    from .dummy import DummyStorage
    engines['dummy'] = DummyStorage
except ImportError:
    pass

try:
    from .s3 import S3Storage
    engines['s3'] = S3Storage
except ImportError:
    pass

try:
    from .gridfs import GridFSStorage
    engines['gridfs'] = GridFSStorage
except ImportError:
    pass
