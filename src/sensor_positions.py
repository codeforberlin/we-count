#!/usr/bin/env python
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

import http.client
import json
import time


def add_camera(conn, headers, res):
    for sens in res["features"]:
        segment = sens["properties"]["segment_id"]
        time.sleep(1)
        conn.request("GET", "/v1/cameras/segment/%s" % segment, '', headers)
        sensors = json.loads(conn.getresponse().read())
        print(sensors)
        sens["properties"]["instance_id"] = sensors["camera"][0]["instance_id"]


def main():
    bbox = "12.78509,52.17841,13.84308,52.82727"  # Berlin as in https://github.com/DLR-TS/sumo-berlin
    conn = http.client.HTTPSConnection("telraam-api.net")
    with open('telraam-token.txt') as token:
        headers = { 'X-Api-Key': token.read() }
    # conn.request("GET", "/v1/cameras", '', headers)
    # conn.request("GET", "/v1/segments/id/348917", '', headers)
    # conn.request("GET", "/v1/reports/traffic_snapshot_live", '', headers)
    payload = '{"time":"live", "contents":"minimal", "area":"%s"}' % bbox
    conn.request("POST", "/v1/reports/traffic_snapshot", payload, headers)
    res = json.loads(conn.getresponse().read())
    with open("sensor-geojson.js", "w") as sensor_js:
        print("var sensors = ", file=sensor_js, end='')
        # add_camera(conn, headers, res)
        json.dump(res, sensor_js, indent=2)
        print(";", file=sensor_js)


if __name__ == "__main__":
    main()
