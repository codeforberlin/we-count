#!/usr/bin/env python3
# Copyright (c) 2023-2024 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    update.py
# @author  Michael Behrisch
# @date    2023-11-27

import json
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
from common import GEO_JSON_NAME


print("Content-Type: text/html\n")
secrets = os.path.join(BASE, "secrets.json")
csv_base = os.path.join(BASE, "..", "csv")
json_path = os.path.join(csv_base, GEO_JSON_NAME)
if sensor_positions.main(["-j", json_path,
                          "--js-file", os.path.join(BASE, "..", "sensor-geojson.js"),
                          "-s", secrets, "-v"]):
    backup_data.main(["-j", json_path,
                      "--csv", os.path.join(csv_base, "bzm_telraam"),
                      "--csv-segments", os.path.join(csv_base, "segments", "bzm_telraam"),
                      "-s", secrets, "-v"])
    kibana_path = os.path.join(csv_base, "kibana", GEO_JSON_NAME)
    with open(json_path, encoding="utf8") as jin, open(kibana_path, "w", encoding="utf8") as jout:
        j = json.load(jin)
        for segment in j["features"]:
            segment["properties"]["segment_id"] = str(segment["properties"]["segment_id"])
        json.dump(j, jout, indent=2)
