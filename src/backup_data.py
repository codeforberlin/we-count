#!/usr/bin/env python
# Copyright (c) 2023 Michael Behrisch
# SPDX-License-Identifier: MIT

# @file    backup_data.py
# @author  Michael Behrisch
# @date    2023-01-11

import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from common import ConnectionProvider, get_options
from datamodel import Base, TrafficCount, Segment, Camera


def get_segments(session, conns, options):
    segments = {}
    bbox = [float(f) for f in options.bbox.split(",")]
    snap = conns.request("/v1/reports/traffic_snapshot_live", required="features")
    # snap = json.load(open('sensor.geojson'))
    for segment in snap.get("features", []):
        inside = False
        if len(segment["geometry"]["coordinates"]) > 1:
            print("Warning! Real multiline for segment", segment["properties"]["segment_id"])
        for p in segment["geometry"]["coordinates"][0]:
            inside = (bbox[0] < p[0] < bbox[2] and bbox[1] < p[1] < bbox[3])
            if not inside:
                break
        if inside:
            sid = segment["properties"]["segment_id"]
            s = session.get(Segment, sid)
            if s is None:
                s = Segment(segment["properties"], segment["geometry"]["coordinates"][0])
            else:
                s.update(segment["properties"])
            segments[sid] = s
    return segments


def get_cameras(session, conns, segments):
    cameras = conns.request("/v1/cameras", required="cameras")
    # cameras = json.load(open('cameras.json'))
    for camera in cameras.get("cameras", []):
        if camera["segment_id"] in segments:
            c = session.get(Camera, camera["instance_id"])
            if c is None:
                segments[camera["segment_id"]].add_camera(camera)


def main(args=None):
    options = get_options(args)
    if "+" not in options.database and "://" not  in options.database:
        options.database = "sqlite+pysqlite:///" + options.database
    engine = create_engine(options.database, echo=options.verbose > 1, future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    conns = ConnectionProvider(options.token_file, options.url)
    segments = get_segments(session, conns, options)
    get_cameras(session, conns, segments)
    session.commit()
    print("Retrieving data for %s segments" % len(segments))
    for s in segments.values():
        session.add(s)
        active = [c.first_data_utc for c in s.cameras if c.first_data_utc is not None]
        if not active:
            print("No active camera for segment %s." % s.id)
            continue
        first = s.last_backup_utc if s.last_backup_utc else min(active)
        last = s.last_data_utc
        if options.verbose and first < last:
            print("Retrieving data for segment %s between %s and %s." % (s.id, first, last))
        while first < last:
            interval_end = first + datetime.timedelta(days=90)
            payload = '{"level": "segments", "format": "per-hour", "id": "%s", "time_start": "%s", "time_end": "%s"}' % (s.id, first, interval_end)
            res = conns.request("/v1/reports/traffic", "POST", payload, options.retry, "report")
            for entry in res.get("report", []):
                if entry["uptime"] > 0:
                    tc = TrafficCount(entry)
                    session.add(tc)
            first = interval_end
        s.last_backup_utc = s.last_data_utc
        session.commit()
    if options.verbose:
        conns.print_stats()


if __name__ == "__main__":
    main()
