import urllib
import boto
from oyster.conf import settings


class S3Storage(object):
    storage_type = 's3'

    def __init__(self, kernel):
        self.kernel = kernel
        self.s3conn = boto.connect_s3(settings.AWS_KEY, settings.AWS_SECRET)
        self._bucket = False

    @property
    def bucket(self):
        if not self._bucket:
            self._bucket = self.s3conn.get_bucket(settings.AWS_BUCKET)
        return self._bucket

    def _get_opt(self, doc_class, setting, default=None):
        """ doc_class first, then setting, then default """
        return self.kernel.doc_classes[doc_class].get(setting,
                                          getattr(settings, setting, default))

    def put(self, tracked_doc, data, content_type):
        """ upload the document to S3 """
        k = boto.s3.key.Key(self.bucket)
        aws_prefix = self._get_opt(tracked_doc['doc_class'], 'AWS_PREFIX', '')
        aws_bucket = self._get_opt(tracked_doc['doc_class'], 'AWS_BUCKET')
        key_name = aws_prefix + tracked_doc['_id']
        k.key = key_name
        headers = {'x-amz-acl': 'public-read',
                   'Content-Type': content_type}
        k.set_contents_from_string(data, headers=headers)
        # can also set metadata if we want, useful?

        url = 'http://%s.s3.amazonaws.com/%s' % (aws_bucket, key_name)
        return url

    def get(self, id):
        # could use get_contents_as_string, any advantages?
        return urllib.urlopen(id).read()
