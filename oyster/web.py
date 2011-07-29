from flask import Flask, make_response
from oyster.client import Client

app = Flask('oyster')

@app.route('/doc/')
def doc_list():
    pass

@app.route('/doc/<path:url>/<version>')
def show_doc(url, version):
    c = Client()
    if version == 'latest':
        version = -1
    doc = c.get_version(url, version)
    resp = make_response(doc.read())
    resp.headers['content-type'] = doc.mimetype
    return resp

if __name__ == '__main__':
    app.run(debug=True)
