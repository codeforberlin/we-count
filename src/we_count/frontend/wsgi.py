# Copyright (c) 2024-2025 Berlin zählt Mobilität
# SPDX-License-Identifier: MIT

# @file    wsgi.py
# @author  Egbert Klaassen
# @author  Michael Behrisch
# @date    2025-12-01

# this is the entry point for WSGI server like gunicorn

import requests
from flask import request, Response

from . import app

GOATCOUNTER_URL = "http://127.0.0.1:9090"

application = app.app.server

@application.route("/goatcounter/", defaults={"path": ""})
@application.route("/goatcounter/<path:path>", methods=["GET", "POST", "PUT", "DELETE"])
def goatcounter_proxy(path):
    resp = requests.request(
        method=request.method,
        url=f"{GOATCOUNTER_URL}/goatcounter/{path}",
        headers={k: v for k, v in request.headers if k.lower() != "host"},
        params=request.query_string.decode(),
        data=request.get_data(),
        stream=True,
        allow_redirects=False,
    )
    return Response(resp.raw.read(), status=resp.status_code, headers=dict(resp.headers))
