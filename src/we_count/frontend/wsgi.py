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

GOATCOUNTER_URL = "http://127.0.0.1:9090/goatcounter"

@app.app.route("/goatcounter/", defaults={"path": ""})
@app.app.route("/goatcounter/<path:path>", methods=["GET", "POST", "PUT", "DELETE"])
def goatcounter_proxy(path):
    resp = requests.request(
        method=request.method,
        url=f"{GOATCOUNTER_URL}/{path}",
        headers={k: v for k, v in request.headers if k.lower() != "host"},
        params=request.query_string.decode(),
        data=request.get_data(),
    )
    return Response(resp.content, status=resp.status_code, headers=dict(resp.headers))

application = app.app.server
