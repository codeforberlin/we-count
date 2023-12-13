#!/usr/bin/env python
# Copyright (c) 2023 Michael Behrisch
# SPDX-License-Identifier: MIT

import csv
import datetime
import gzip
import os

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


def update_db(segments, session, options, conns):
    print("Retrieving data for %s segments" % len(segments))
    newest_data = None
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
        if newest_data is None or newest_data < s.last_data_utc:
            newest_data = s.last_data_utc
        session.commit()
    return newest_data


def main(args=None):
    options = get_options(args)
    engine = create_engine(options.database, echo=options.verbose > 1, future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    conns = ConnectionProvider(options.tokens, options.url)
    segments = get_segments(session, conns, options)
    get_cameras(session, conns, segments)
    session.commit()
    newest_data = update_db(segments, session, options, conns)
    if options.csv:
        if options.csv[-1] != "/":
            os.makedirs(os.path.dirname(options.csv), exist_ok=True)
        curr_month = (newest_data.year, newest_data.month)
        month = (options.csv_start_year, 1) if options.csv_start_year else (curr_month[0] - 1, curr_month[1])
        while month <= curr_month:
            with gzip.open(options.csv + "_%s_%02i.csv.gz" % month, "wt") as csv_file:
                csv_out = csv.writer(csv_file)
                need_header = True
                for s in segments.values():
                    for tc in s.counts:
                        if (tc.date_utc.year, tc.date_utc.month) == month:
                            if need_header:
                                csv_out.writerow(tc.get_column_names())
                                need_header = False
                            csv_out.writerow(tc.get_column_values())
            if need_header:  # no data
                os.remove(csv_file.name)
            month = (month[0] if month[1] < 12 else month[0] + 1,
                     month[1] + 1 if month[1] < 12 else 1)

    if options.csv_segments:
        if options.csv_segments[-1] != "/":
            os.makedirs(os.path.dirname(options.csv_segments), exist_ok=True)
        for s in segments.values():
            with gzip.open(options.csv_segments + "_%s.csv.gz" % s.id, "wt") as csv_file:
                csv_out = csv.writer(csv_file)
                need_header = True
                for tc in s.counts:
                    if need_header:
                        csv_out.writerow(tc.get_column_names())
                        need_header = False
                    csv_out.writerow(tc.get_column_values())

    if options.verbose:
        conns.print_stats()


if __name__ == "__main__":
    main()
