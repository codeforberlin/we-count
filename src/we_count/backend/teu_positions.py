#!/usr/bin/env python
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    teu_positions.py
# @author  Michael Behrisch
# @date    2026-03-01

import datetime
import sys

import osm
from common import fetch_all, get_options, load_json_if_stale, save_json


DEFAULT_URL = "https://api.viz.berlin.de/FROST-Server-TEU/v1.1"


def main(args=None):
    options = get_options(args, json_default="teu.json", url_default=DEFAULT_URL)
    old_features = load_json_if_stale(options.json_file, options.clear, options.verbose)
    if old_features is None:
        return False
    old_data = {f["properties"]["segment_id"]: f["properties"] for f in old_features}

    all_things = fetch_all(options.url + "/Things", {"$select": "@iot.id,name,description,properties"})
    if options.verbose:
        print(f"{len(all_things)} stations found.")
    features = []
    for thing in all_things:
        tid = thing["@iot.id"]
        props = thing.get("properties", {})

        segment_id = props.get("mq_id15")
        if segment_id is None:
            print(f"Warning: no mq_id15 for {thing.get('name', tid)}", file=sys.stderr)
            continue

        # Fetch MQ datastreams, organize by vehicle → measurement → period
        datastreams_raw = fetch_all(
            options.url + f"/Things({tid})/Datastreams",
            {"$select": "@iot.id,properties", "$filter": "properties/lane eq 'MQ'"}
        )
        datastreams = {}
        for ds in datastreams_raw:
            dp = ds.get("properties", {})
            vehicle = dp.get("vehicle", "")
            measurement = dp.get("measurement", "")
            period = dp.get("periodLength", "")
            datastreams.setdefault(vehicle, {}).setdefault(measurement, {})[period] = ds["@iot.id"]

        # Get coordinates from Thing location
        locs = fetch_all(options.url + f"/Things({tid})/Locations", {"$select": "location"})
        coords = None
        if locs:
            geo = locs[0].get("location", {})
            if geo.get("type") == "Point":
                coords = geo["coordinates"]

        existing = old_data.get(segment_id, {})
        feature_props = {
            **props,
            "description": thing.get("description", ""),
            "name": thing.get("name", ""),
            "segment_id": segment_id,
            "thing_id": tid,
            "timezone": "Europe/Berlin",
            "datastreams": datastreams,
            "last_data_backup": existing.get("last_data_backup"),
            "last_advanced_backup": existing.get("last_advanced_backup"),
        }
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": coords} if coords else None,
            "properties": feature_props,
        })

    osm.add_osm(features, old_data)
    result = {
        "type": "FeatureCollection",
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "features": features,
    }
    save_json(options.json_file, result)
    return True


if __name__ == "__main__":
    main()
