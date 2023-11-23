#!/usr/bin/env python3
# Copyright (c) 2023 Michael Behrisch
# SPDX-License-Identifier: MIT

# @file    sensor_positions.py
# @author  Michael Behrisch
# @date    2023-01-03

import os
import json
import datetime

from common import ConnectionProvider, get_options


def add_camera(conns, res):
    for sens in res["features"]:
        segment = sens["properties"]["segment_id"]
        sensors = conns.request("/v1/cameras/segment/%s" % segment)
        print(sensors)
        sens["properties"]["instance_id"] = sensors["camera"][0]["instance_id"]


def main(args=None):
    options = get_options(args)
    if options.web_server:
        print("Content-Type: text/html\n")
    if os.path.exists(options.json_file):
        last_mod = datetime.datetime.fromtimestamp(os.path.getmtime(options.json_file))
        delta = datetime.timedelta(hours=1)
        if datetime.datetime.now() - last_mod < delta:
            if options.verbose:
                print(f"Not recreating {options.json_file}, it is less than {delta} old.")
            return
    conns = ConnectionProvider(options.token_file, options.url)
    payload = '{"time":"live", "contents":"minimal", "area":"%s"}' % options.bbox
    res = conns.request("/v1/reports/traffic_snapshot", "POST", payload)
    with open(options.json_file, "w", encoding="utf8") as sensor_js:
        if options.verbose and "features" in res:
            print(f"{len(res['features'])} sensor positions read.")
        print("var sensors = ", file=sensor_js, end='')
        if options.camera:
            add_camera(conns, res)
        json.dump(res, sensor_js, indent=2)
        print(";", file=sensor_js)


if __name__ == "__main__":
    main()
