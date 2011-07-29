import flask
from oyster.client import Client

app = flask.Flask('oyster')
client = Client()

@app.route('/status/')
def doc_list():
    status = {
        'queue_size': app.work_queue.qsize(),
        'tracking': client.db.tracked.count(),
        'need_update': client.get_update_queue_size(),
    }
    return flask.jsonify(**status)


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
