from flask import Flask
from .client import Client

app = Flask('oyster')

@app.route('/doc/')
def doc_list():
    pass

@app.route('/doc/<path:url>/<version>')
def show_doc(url, version):
    c = Client()
    if version == 'latest':
        version = -1
    return c.get_version(url, version).read()

if __name__ == '__main__':
    app.run()
