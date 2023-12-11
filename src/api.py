from flask import Blueprint, jsonify, send_from_directory

json_api = Blueprint('json_api', __name__)

@json_api.route('/api')
def test():
    return jsonify({'name': 'alice',
                    'email': 'alice@bob.com'})

csv_api = Blueprint('csv_api', __name__)

@json_api.route('/csv/<path:path>')
def csv_test(path):
    return send_from_directory('csv', path)
