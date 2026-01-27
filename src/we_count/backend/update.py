#!/usr/bin/env python3
# Copyright (c) 2023-2024 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    update.py
# @author  Michael Behrisch
# @date    2023-11-27

import datetime
import glob
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
import bzm_get_data
from common import GEO_JSON_NAME


print("Content-Type: text/html\n")
secrets = os.path.join(BASE, "secrets.json")
# csv_base = os.path.join("assets")
csv_base = os.path.join(BASE, "..", "csv")
json_path = os.path.join(csv_base, GEO_JSON_NAME)
if sensor_positions.main(["-j", json_path,
                          "-s", secrets, "-v"]):
    backup_data.main(["-j", json_path,
                      "--csv", os.path.join(csv_base, "bzm_telraam"),
                      "--csv-segments", os.path.join(csv_base, "segments", "bzm_telraam"),
                      "-s", secrets, "-v"])
    bzm_get_data.main(["-v"])
last_update = datetime.datetime.now(datetime.timezone.utc).isoformat(" ")[:-16]
for jf in glob.glob(os.path.join(csv_base, "*.geojson")):
    kibana_path = os.path.join(csv_base, "kibana", os.path.basename(jf))
    with open(jf, encoding="utf8") as jin, open(kibana_path, "w", encoding="utf8") as jout:
        j = json.load(jin)
        for segment in j["features"]:
            s = {"last_update": last_update, "geometry": segment["geometry"]}
            s.update(segment["properties"])
            s["segment_id"] = str(s["segment_id"])
            if "osm" in s:
                s["osm"]["osmid"] = str(s["osm"]["osmid"])
            if s.get("cameras"):
                cams = []
                for c in s["cameras"]:
                    cs = {}
                    for k, v in c.items():
                        cs[k] = str(v) if k[-3:] in ("mac", "_id") else v
                    cams.append(cs)
                s["cameras"] = cams
            if s.get("cameras") or s.get("eco-counter"):
                json.dump(s, jout)
                jout.write("\n")
