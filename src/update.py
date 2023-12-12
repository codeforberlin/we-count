#!/usr/bin/env python3
# Copyright (c) 2023 Michael Behrisch
# SPDX-License-Identifier: MIT

import os
import sys

BASE = os.path.abspath(os.path.dirname(__file__))
for p in sys.path:
    if p.endswith('site-packages'):
        venv = os.path.join(BASE, "..", "..", "venv_wecount", p[p.index("/lib/") + 1:])
        sys.path = [venv] + sys.path
        break

import sensor_positions
import backup_data


print("Content-Type: text/html\n")
secrets = os.path.join(BASE, "secrets.json")
if sensor_positions.main(["-j", os.path.join(BASE, "assets", "sensor.json"),
                          "--js-file", os.path.join(BASE, "..", "sensor-geojson.js"),
                          "-s", secrets, "-v"]):
    backup_data.main(["-s", secrets, "-v"])
