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

# @file    backup_data.py
# @author  Michael Behrisch
# @date    2023-01-11

import http.client
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from datamodel import Base, TrafficCount

def main():
    conn = http.client.HTTPSConnection("telraam-api.net")
    with open('telraam-token.txt') as token:
        headers = { 'X-Api-Key': token.read() }
    payload = '{"level": "segments", "format": "per-hour", "id": "348917", "time_start": "2020-10-30 07:00:00Z", "time_end": "2020-12-30 09:00:00Z"}'
    conn.request("POST", "/v1/reports/traffic", payload, headers)
    res = json.loads(conn.getresponse().read())

    engine = create_engine("sqlite+pysqlite:///test.db", echo=True, future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    for entry in res["report"]:
        if entry["uptime"] > 0:
            tc = TrafficCount(entry)
            session.add(tc)
    session.commit()


if __name__ == "__main__":
    main()
