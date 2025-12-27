# Copyright (c) 2024-2025 Berlin zählt Mobilität
# SPDX-License-Identifier: MIT

# @file    wsgi.py
# @author  Egbert Klaassen
# @author  Michael Behrisch
# @date    2025-12-01

# this is the entry point for WSGI server like gunicorn

from . import app
application = app.app.server
