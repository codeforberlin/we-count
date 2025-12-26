# Copyright (c) 2023 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    api.py
# @author  Michael Behrisch
# @date    2023-12-11

from flask import Blueprint, jsonify, send_file, request

json_api = Blueprint('json_api', __name__)

@json_api.route('/api/test')
def test():
    return jsonify({'name': 'alice',
                    'email': 'alice@bob.com'})

@json_api.route('/api/v1/reports/traffic_snapshot_live')
def traffic_snapshot_live():
    return send_file('assets/segments.geojson', mimetype="application/json")

@json_api.route('/api/v1/reports/traffic')
def traffic():
    payload = request.json
    return jsonify(payload)
