from flask import Blueprint, jsonify, send_file

json_api = Blueprint('json_api', __name__)

@json_api.route('/api/test')
def test():
    return jsonify({'name': 'alice',
                    'email': 'alice@bob.com'})

@json_api.route('/api/v1/reports/traffic_snapshot_live')
def traffic_snapshot_live():
    return send_file('assets/segments.geojson', mimetype="application/json")
