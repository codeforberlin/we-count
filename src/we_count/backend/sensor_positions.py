#!/usr/bin/env python3
# Copyright (c) 2023-2024 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    sensor_positions.py
# @author  Michael Behrisch
# @date    2023-01-15

import os
import json
import datetime
import sys

import osm
from common import ConnectionProvider, get_options


def add_osm(res, old_data):
    now = datetime.datetime.now(datetime.UTC)
    update_count = 0  # do not update too many at once, it is costly
    for segment in res["features"]:
        segment_id = segment["properties"]["segment_id"]
        if segment_id in old_data and "osm" in old_data[segment_id]["properties"]:
            osm_edge = old_data[segment_id]["properties"]["osm"]
            if "name" in osm_edge:
                # use cached data if it is not outdated
                last_osm_update = datetime.datetime.fromisoformat(osm_edge.get("last_osm_fetch", '1970-01-01 00:00:00+00:00'))
                if last_osm_update > now - datetime.timedelta(days=30):
                    segment["properties"]["osm"] = osm_edge
                    continue
        update_count += 1
        osm_edge = osm.find_edge(segment)
        osm_edge["last_osm_fetch"] = now.isoformat(" ")
        del osm_edge["geometry"]
        segment["properties"]["osm"] = osm_edge
        if update_count > 1:
            break


def update_props(bbox_segments, old_data, conns, retry):
    now = datetime.datetime.now(datetime.UTC)
    update_count = 0  # do not update too many at once, it is costly
    new_segments = []
    for segment_id in sorted(bbox_segments):
        if segment_id in old_data:
            old_segment = old_data[segment_id]
            last_prop_update = datetime.datetime.fromisoformat(old_segment["properties"].get("last_prop_fetch", '1970-01-01 00:00:00+00:00'))
            if update_count > 10 or last_prop_update > now - datetime.timedelta(days=1):
                new_segments.append(old_segment)
                continue
        update_count += 1
        segment_data = conns.request("/v1/segments/id/%s" % segment_id, retries=retry, required="features")
        if not segment_data.get('features'):
            continue
        segment = segment_data["features"][0]
        segment["properties"] = {"segment_id": segment_id, "last_prop_fetch": now.isoformat(" ")} | segment["properties"]
        new_segments.append(segment)
    invalid = set(old_data.keys()) - bbox_segments
    if invalid:
        print(f"The following segments are not in the database: {invalid}.", file=sys.stderr)
        for s in invalid:
            new_segments.append(old_data[s])
    return new_segments


def main(args=None):
    options = get_options(args)
    old_data = {}
    if os.path.exists(options.json_file) and not options.clear:
        last_mod = datetime.datetime.fromtimestamp(os.path.getmtime(options.json_file))
        delta = datetime.timedelta(minutes=30)
        if datetime.datetime.now() - last_mod < delta:
            if options.verbose:
                print(f"Not recreating {options.json_file}, it is less than {delta} old.")
            return False
        with open(options.json_file, encoding="utf8") as of:
            old_data = {s["properties"]["segment_id"] : s for s in json.load(of)["features"]}

    conns = ConnectionProvider(options.secrets["tokens"], options.url)
    res = conns.request("/v1/segments/area", "POST", str({"area": options.bbox}), retries=options.retry, required="features")
    bbox_segments = set(f["properties"]["segment_id"] for f in res.get("features", []))
    if options.verbose:
        print(f"{len(bbox_segments)} total sensor positions in the bounding box.")

    res = {
        "we_count_version": 2,
        "description": f"Telraam segments and instances for {options.bbox} enhanced with OpenStreetMap data, "
        "format description at https://app.swaggerhub.com/apis-docs/telraam/Telraam-API/1.2.0#/Segments/get_v1_segments_id__segment_id_",
        "created_at": datetime.datetime.now(datetime.UTC).isoformat(" "),
        "type": "FeatureCollection",
        "features": update_props(bbox_segments, old_data, conns, options.retry)
    }
    add_osm(res, old_data)
    with open(options.json_file, "w", encoding="utf8") as segment_json:
        json.dump(res, segment_json, indent=2)
    return True


if __name__ == "__main__":
    main()
