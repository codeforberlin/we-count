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

with open('../umriss_tk.geojson') as tk_json:
    tk = json.load(tk_json)
    bbox = [1000, 1000, -1000, -1000]
    for line in tk["features"][0]["geometry"]["coordinates"]:
        for p in line:
            bbox[0] = min(bbox[0], p[0])
            bbox[1] = min(bbox[1], p[1])
            bbox[2] = max(bbox[2], p[0])
            bbox[3] = max(bbox[3], p[1])
print(bbox)
conn = http.client.HTTPSConnection("telraam-api.net")
with open('telraam-token.txt') as token:
    headers = { 'X-Api-Key': token.read() }
# conn.request("GET", "/v1/cameras", '', headers)
# conn.request("GET", "/v1/segments/id/348917", '', headers)
payload = '{"time":"live", "contents":"minimal", "area":"%s"}' % (str(bbox)[1:-1])
conn.request("POST", "/v1/reports/traffic_snapshot", payload, headers)
data = conn.getresponse().read().decode("utf-8")
with open("sensor-geojson.js", "w") as sensor_js:
    print("var sensors = ", file=sensor_js, end='')
    res = json.loads(data)
    json.dump(res, sensor_js, indent=2)
    print(";", file=sensor_js)
