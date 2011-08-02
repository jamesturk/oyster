import json
import datetime

import flask
import pymongo.objectid

from oyster.client import Client


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, pymongo.objectid.ObjectId):
            return str(obj)
        else:
            return super(JSONEncoder, self).default(obj)


app = flask.Flask('oyster')
client = Client()

@app.route('/status/')
def doc_list():
    status = {
        'queue_size': app.work_queue.qsize(),
        'tracking': client.db.tracked.count(),
        'need_update': client.get_update_queue_size(),
    }
    return json.dumps(status)


@app.route('/log/')
def log_view():
    offset = int(flask.request.args.get('offset', 0))
    size = 100
    prev_offset = max(offset - size, 0)
    next_offset = offset + size
    logs = client.db.logs.find().sort('$natural', -1).skip(offset).limit(size)
    return flask.render_template('logs.html', logs=logs,
                                 prev_offset=prev_offset,
                                 next_offset=next_offset,
                                 offset=offset)


@app.route('/tracked/')
def tracked():
    tracked = list(client.db.tracked.find())
    return json.dumps(tracked, cls=JSONEncoder)

@app.route('/tracked/<path:url>')
def tracked_view(url):
    doc = client.db.tracked.find_one({'url': url})
    return json.dumps(doc, cls=JSONEncoder)


@app.route('/doc/<path:url>/<version>')
def show_doc(url, version):
    if version == 'latest':
        version = -1
    doc = client.get_version(url, version)
    resp = flask.make_response(doc.read())
    resp.headers['content-type'] = doc.mimetype
    return resp


if __name__ == '__main__':
    app.run(debug=True)
