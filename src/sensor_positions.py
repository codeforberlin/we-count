#!/usr/bin/env python3
# Copyright (c) 2023-2024 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    sensor_positions.py
# @author  Michael Behrisch
# @date    2023-01-15

import os
import json
import datetime

import pyproj
import timezonefinder

import osm
from common import ConnectionProvider, get_options


def add_camera(conns, res):
    for segment in res["features"]:
        segment_id = segment["properties"]["segment_id"]
        cameras = conns.request("/v1/cameras/segment/%s" % segment_id)
        segment["properties"]["cameras"] = cameras["camera"]


def add_osm(res, old_data):
    lookup = {s["properties"]["segment_id"] : s for s in old_data["features"]}
    for segment in res["features"]:
        segment_id = segment["properties"]["segment_id"]
        if segment_id in lookup and "osm" in lookup[segment_id]["properties"]:
            osm_edge = lookup[segment_id]["properties"]["osm"]
        else:
            osm_edge = osm.find_edge(segment)
            del osm_edge["geometry"]
        segment["properties"]["osm"] = osm_edge


def main(args=None):
    options = get_options(args)
    if os.path.exists(options.js_file):
        last_mod = datetime.datetime.fromtimestamp(os.path.getmtime(options.js_file))
        delta = datetime.timedelta(minutes=30)
        if datetime.datetime.now() - last_mod < delta:
            if options.verbose:
                print(f"Not recreating {options.js_file}, it is less than {delta} old.")
            return False
    conns = ConnectionProvider(options.secrets["tokens"], options.url)

    old_data = no_data = {'features': []}
    if os.path.exists(options.json_file):
        with open(options.json_file, encoding="utf8") as of:
            old_data = json.load(of)

    bbox = [float(f) for f in options.bbox.split(",")]
    transformer = pyproj.Transformer.from_crs("EPSG:31370", "EPSG:4326")
    all_segments = conns.request("/v1/segments/all")
    bbox_segments = set()
    for segment in all_segments["features"]:
        segment_id = segment["properties"]["oidn"]
        inside = False
        if len(segment["geometry"]["coordinates"]) > 1:
            print("Warning! Real multiline for segment", segment_id)
        wgs_coords = [transformer.transform(*p) for p in segment["geometry"]["coordinates"][0]]
        for lat, lon in wgs_coords:
            inside = (bbox[0] < lon < bbox[2] and bbox[1] < lat < bbox[3])
            if not inside:
                break
        if inside:
            bbox_segments.add(segment_id)
    if options.verbose:
        print(f"{len(bbox_segments)} total sensor positions in the bounding box.")

    res = conns.request("/v1/reports/traffic_snapshot_live", required="features")
    features = res.get("features")
    if not features:
        return
    if options.verbose:
        print(f"{len(res['features'])} live sensor positions worldwide.")
    combined_segments = []
    for segment in features + old_data["features"]:
        segment_id = segment["properties"]["segment_id"]
        if segment_id in bbox_segments:
            combined_segments.append(segment)
            bbox_segments.remove(segment_id)
    tf = timezonefinder.TimezoneFinder()
    for segment_id in bbox_segments:
        segment_data = conns.request("/v1/segments/id/%s" % segment_id, required="features")
        if 'features' not in segment_data:
            continue
        segment = segment_data["features"][0]
        wgs_coords = segment["geometry"]["coordinates"][0]
        timezone = tf.timezone_at(lng=wgs_coords[0][0], lat=wgs_coords[0][1])
        new_props = {"segment_id": segment["properties"]["oidn"], "timezone": timezone}
        for key, value in segment["properties"].items():
            if key not in ("road_speed", "road_type", "speed_histogram", "speed_buckets", "oidn"):
                new_props[key] = value
        segment["properties"] = new_props
        combined_segments.append(segment)
    res["features"] = combined_segments
    add_osm(res, no_data if options.osm else old_data)
    add_camera(conns, res)
    with open(options.json_file, "w", encoding="utf8") as segment_json:
        json.dump(res, segment_json, indent=2)
    with open(options.js_file, "w", encoding="utf8") as sensor_js:
        print("var sensors = ", file=sensor_js, end='')
        json.dump(res, sensor_js, indent=2)
        print(";", file=sensor_js)
    return True


if __name__ == "__main__":
    main()
