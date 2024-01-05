from flask import Blueprint, jsonify

json_api = Blueprint('json_api', __name__)

@json_api.route('/api/<path:path>')
def test(path):
    return jsonify({'name': 'alice',
                    'email': 'alice@bob.com'})
