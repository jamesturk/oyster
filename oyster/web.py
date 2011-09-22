import re
import json
import datetime
import functools

import flask
import pymongo.objectid

from oyster.conf import settings
from oyster.client import get_configured_client


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, pymongo.objectid.ObjectId):
            return str(obj)
        else:
            return super(JSONEncoder, self).default(obj)

def _path_fixer(url):
    """ this exists because werkzeug seems to collapse // into / sometimes
        certainly a hack, but given that werkzeug seems to only do the mangling
        *sometimes* being a bit aggressive was the only viable option
    """
    return re.sub(r'(http|https|ftp):/([^/])', r'\1://\2', url)

def api_wrapper(template=None):
    def wrapper(func):
        @functools.wraps(func)
        def newfunc(*args, **kwargs):
            data = func(*args, **kwargs)
            if 'json' in flask.request.args or not template:
                return json.dumps(data, cls=JSONEncoder)
            else:
                return flask.render_template(template, **data)

        return newfunc
    return wrapper


app = flask.Flask('oyster')
client = get_configured_client()


@app.route('/')
@api_wrapper('index.html')
def index():
    status = {
        'tracking': client.db.tracked.count(),
        'need_update': client.get_update_queue_size(),
        'logs': client.db.logs.find().sort('$natural', -1).limit(20),
        'mongo_host': settings.MONGO_HOST,
    }
    return status


@app.route('/status/')
@api_wrapper()
def doc_list():
    status = {
        'tracking': client.db.tracked.count(),
        'need_update': client.get_update_queue_size(),
    }
    return status


@app.route('/log/')
@api_wrapper('logs.html')
def log_view():
    offset = int(flask.request.args.get('offset', 0))
    size = 100
    prev_offset = max(offset - size, 0)
    next_offset = offset + size
    logs = client.db.logs.find().sort('$natural', -1).skip(offset).limit(size)
    return dict(logs=list(logs), prev_offset=prev_offset,
                next_offset=next_offset, offset=offset)


@app.route('/tracked/')
@api_wrapper()
def tracked():
    tracked = list(client.db.tracked.find())
    return json.dumps(tracked, cls=JSONEncoder)


@app.route('/tracked/<id>')
def tracked_view(id):
    doc = client.db.tracked.find_one({'_id': pymongo.objectid.ObjectId(id)})
    return json.dumps(doc, cls=JSONEncoder)


@app.route('/doc/<path:url>/<version>')
def show_doc(url, version):
    url = _path_fixer(url)
    if version == 'latest':
        version = -1
    doc = client.get_version(url, version)
    resp = flask.make_response(doc.read())
    resp.headers['content-type'] = doc.content_type
    return resp


if __name__ == '__main__':
    app.run(debug=True)
