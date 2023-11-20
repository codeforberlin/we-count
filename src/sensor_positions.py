#!/usr/bin/env python3
# Copyright (c) 2023 Michael Behrisch
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# SPDX-License-Identifier: MIT

# @file    sensor_positions.py
# @author  Michael Behrisch
# @date    2023-01-03

import os
import http.client
import json
import time
import datetime
import argparse


def get_options(args=None):
    base = os.path.abspath(os.path.dirname(__file__))
    parser = argparse.ArgumentParser()
    # Berlin as in https://github.com/DLR-TS/sumo-berlin
    parser.add_argument("-b", "--bbox", default="12.78509,52.17841,13.84308,52.82727",
                        help="bounding box to retrieve in geo coordinates west,south,east,north")
    parser.add_argument("-u", "--url", default="telraam-api.net",
                        help="Download from the given Telraam server")
    parser.add_argument("-t", "--token-file", default=os.path.join(base, "telraam-token.txt"),
                        metavar="FILE", help="Read Telraam API token from FILE")
    parser.add_argument("-j", "--json-file", default=os.path.join(base, "..", "sensor-geojson.js"),
                        metavar="FILE", help="Write Geo-JSON output to FILE")
    parser.add_argument("--camera", action="store_true", default=False,
                        help="include individual cameras")
    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="enable verbose output")
    return parser.parse_args(args=args)


def add_camera(conn, headers, res):
    for sens in res["features"]:
        segment = sens["properties"]["segment_id"]
        time.sleep(1)
        conn.request("GET", "/v1/cameras/segment/%s" % segment, '', headers)
        sensors = json.loads(conn.getresponse().read())
        print(sensors)
        sens["properties"]["instance_id"] = sensors["camera"][0]["instance_id"]


def main():
    options = get_options()
    if os.path.exists(options.json_file):
        last_mod = datetime.datetime.fromtimestamp(os.path.getmtime(options.json_file))
        delta = datetime.timedelta(hours=1)
        if datetime.datetime.now() - last_mod < delta:
            if options.verbose:
                print(f"Not recreating {options.json_file}, it is less than {delta} old.")
            return
    conn = http.client.HTTPSConnection(options.url)
    with open(options.token_file, encoding="utf8") as token:
        headers = { 'X-Api-Key': token.read().strip() }
    payload = '{"time":"live", "contents":"minimal", "area":"%s"}' % options.bbox
    conn.request("POST", "/v1/reports/traffic_snapshot", payload, headers)
    res = json.loads(conn.getresponse().read())
    with open(options.json_file, "w", encoding="utf8") as sensor_js:
        print("var sensors = ", file=sensor_js, end='')
        if options.camera:
            add_camera(conn, headers, res)
        json.dump(res, sensor_js, indent=2)
        print(";", file=sensor_js)


if __name__ == "__main__":
    main()
