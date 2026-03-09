# Copyright (c) 2024-2026 Berlin zählt Mobilität
# SPDX-License-Identifier: MIT

# @file    wsgi.py
# @author  Egbert Klaassen
# @author  Michael Behrisch
# @date    2025-12-01

# this is the entry point for a WSGI server like gunicorn
# it also redirects requests to a running goatcounter

import requests
from flask import request, Response

from . import app

application = app.app.server

@application.route("/goatcounter/", defaults={"path": ""})
@application.route("/goatcounter/<path:path>", methods=["GET", "POST", "PUT", "DELETE"])
def goatcounter_proxy(path):
    resp = requests.request(
        method=request.method,
        url=f"http://127.0.0.1:9090/goatcounter/{path}",
        headers={k: v for k, v in request.headers if k.lower() != "host"},
        params=request.query_string.decode(),
        data=request.get_data(),
        stream=True,
        allow_redirects=False,
    )
    return Response(resp.raw.read(), status=resp.status_code, headers=dict(resp.headers))
