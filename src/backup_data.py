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

import sys
import http.client
import json
import datetime
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from datamodel import Base, TrafficCount, Segment, Camera


def get_segments(session, conn, headers):
    segments = {}
    bbox = (12.78509,52.17841,13.84308,52.82727)  # Berlin as in https://github.com/DLR-TS/sumo-berlin
    # utc_old = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=30)
    conn.request("GET", "/v1/reports/traffic_snapshot_live", '', headers)
    snap = json.loads(conn.getresponse().read())
    # snap = json.load(open('sensor.geojson'))
    if not "features" in snap:
        print("Format error: %s." % snap.get("message"), file=sys.stderr)
        return
    for segment in snap["features"]:
        inside = False
        if len(segment["geometry"]["coordinates"]) > 1:
            print("Warning! Real multiline for segment", segment["properties"]["segment_id"])
        for p in segment["geometry"]["coordinates"][0]:
            inside = (bbox[0] < p[0] < bbox[2] and bbox[1] < p[1] < bbox[3])
            if not inside:
                break
        if inside:
            id = segment["properties"]["segment_id"]
            s = session.get(Segment, id)
            if s is None:
                s = Segment(segment["properties"], segment["geometry"]["coordinates"][0])
            segments[id] = s
    return segments


def get_cameras(session, conn, headers, segments):
    time.sleep(1)
    conn.request("GET", "/v1/cameras", '', headers)
    cameras = json.loads(conn.getresponse().read())
    # cameras = json.load(open('cameras.json'))
    if not "cameras" in cameras:
        print("Format error: %s." % cameras.get("message"), file=sys.stderr)
        return
    for camera in cameras["cameras"]:
        if camera["segment_id"] in segments:
            id = camera["instance_id"]
            c = session.get(Camera, id)
            if c is None:
                segments[camera["segment_id"]].add_camera(camera)


def main():
    engine = create_engine("sqlite+pysqlite:///backup.db", echo=True, future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    conn = http.client.HTTPSConnection("telraam-api.net")
    with open('telraam-token.txt') as token:
        headers = { 'X-Api-Key': token.read().strip() }
    segments = get_segments(session, conn, headers)
    get_cameras(session, conn, headers, segments)
    print("retrieving data for %s segments" % len(segments))
    for s in segments.values():
        session.add(s)
        active = [c.first_data_utc for c in s.cameras if c.first_data_utc is not None]
        if not active:
            print("no active camera for segment %s." % s.id)
            continue
        first = s.last_backup_utc if s.last_backup_utc else min(active)
        last = s.last_data_utc
        print(first, last)
        while first < last:
            time.sleep(1)
            interval_end = first + datetime.timedelta(days=90)
            payload = '{"level": "segments", "format": "per-hour", "id": "%s", "time_start": "%s", "time_end": "%s"}' % (s.id, first, interval_end)
            conn.request("POST", "/v1/reports/traffic", payload, headers)
            res = json.loads(conn.getresponse().read())
            if not "report" in res:
                print("Format error: %s." % res.get("message"), file=sys.stderr)
                continue
            for entry in res["report"]:
                if entry["uptime"] > 0:
                    tc = TrafficCount(entry)
                    session.add(tc)
            first = interval_end
        s.last_backup_utc = s.last_data_utc
    session.commit()


if __name__ == "__main__":
    main()
