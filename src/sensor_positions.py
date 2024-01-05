#!/usr/bin/env python3
# Copyright (c) 2023 Michael Behrisch
# SPDX-License-Identifier: MIT

# @file    sensor_positions.py
# @author  Michael Behrisch
# @date    2023-01-03

import os
import json
import datetime

import osm
from common import ConnectionProvider, get_options


def add_camera(conns, res):
    for sens in res["features"]:
        segment = sens["properties"]["segment_id"]
        sensors = conns.request("/v1/cameras/segment/%s" % segment)
        print(sensors)
        sens["properties"]["instance_id"] = sensors["camera"][0]["instance_id"]


def add_osm(res):
    for s in res["features"]:
        edge = osm.find_edge(s)
        del edge["geometry"]
        s["properties"]["osm"] = edge


def main(args=None):
    options = get_options(args)
    if os.path.exists(options.js_file):
        last_mod = datetime.datetime.fromtimestamp(os.path.getmtime(options.js_file))
        delta = datetime.timedelta(hours=1)
        if datetime.datetime.now() - last_mod < delta:
            if options.verbose:
                print(f"Not recreating {options.js_file}, it is less than {delta} old.")
            return False
    conns = ConnectionProvider(options.tokens, options.url)
    payload = '{"time":"live", "contents":"minimal", "area":"%s"}' % options.bbox
    res = conns.request("/v1/reports/traffic_snapshot", "POST", payload)
    features = res.get("features")
    if not features:
        print("no features")
        return
    if options.verbose:
        print(f"{len(res['features'])} sensor positions read.")
    add_osm(res)
    if options.camera:
        add_camera(conns, res)
    for f in options.json_file:
        with open(f, "w", encoding="utf8") as segment_json:
            json.dump(res, segment_json, indent=2)
    with open(options.js_file, "w", encoding="utf8") as sensor_js:
        print("var sensors = ", file=sensor_js, end='')
        json.dump(res, sensor_js, indent=2)
        print(";", file=sensor_js)
    return True


if __name__ == "__main__":
    main()
