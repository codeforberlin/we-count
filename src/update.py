#!/usr/bin/env python3
# Copyright (c) 2023 Michael Behrisch
# SPDX-License-Identifier: MIT

# @file    update.py
# @author  Michael Behrisch
# @date    2023-11-23

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
sensor_file = os.path.join(BASE, "..", "sensor-geojson.js")
if sensor_positions.main(["-j", sensor_file, "-s", os.path.join(BASE, "secrets.json"), "-v"]):
    backup_data.main(["-s", os.path.join(BASE, "secrets.json"), "-v"])
