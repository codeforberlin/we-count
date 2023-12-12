#!/usr/bin/env python3
# Copyright (c) 2023 Michael Behrisch
# SPDX-License-Identifier: MIT

import os
import sys
from wsgiref.handlers import CGIHandler

BASE = os.path.abspath(os.path.dirname(__file__))
for p in sys.path:
    if p.endswith('site-packages'):
        venv = os.path.join(BASE, "..", "..", "venv_wecount", p[p.index("/lib/") + 1:])
        sys.path = [venv] + sys.path
        break

from map import app

CGIHandler().run(app.server)
