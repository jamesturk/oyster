import urllib
import boto
from oyster.conf import settings


class S3Storage(object):
    storage_type = 's3'

    def __init__(self, kernel):
        self.s3conn = boto.connect_s3(settings.AWS_KEY, settings.AWS_SECRET)

    def put(self, tracked_doc, data, content_type):
        """ upload the document to S3 """
        bucket = self.s3conn.create_bucket(settings.AWS_BUCKET)
        k = boto.s3.key.Key(bucket)
        k.key = tracked_doc['_id']
        headers = {'x-amz-acl': 'public-read',
                   'Content-Type': content_type}
        k.set_contents_from_string(data, headers=headers)
        # can also set metadata if we want, useful?

        url = 'http://%s.s3.amazonaws.com/%s' % (settings.AWS_BUCKET,
                                                 tracked_doc['_id'])
        return url

    def get(self, id):
        # could use get_contents_as_string, any advantages?
        return urllib.urlopen(id).read()