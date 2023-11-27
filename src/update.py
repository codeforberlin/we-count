#!/usr/bin/env python3
# Copyright (c) 2023 Michael Behrisch
# SPDX-License-Identifier: MIT

# @file    update.py
# @author  Michael Behrisch
# @date    2023-11-23

import os

import sensor_positions
import backup_data

print("Content-Type: text/html\n")
base = os.path.abspath(os.path.dirname(__file__))
sensor_file = os.path.join(base, "..", "sensor-geojson.js")
if sensor_positions.main(["-j", sensor_file, "-s", os.path.join(base, "secrets.json"), "-v"]):
    backup_data.main(["-s", os.path.join(base, "secrets.json"), "-v"])
