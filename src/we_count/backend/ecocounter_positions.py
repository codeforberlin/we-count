#!/usr/bin/env python
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    ecocounter_positions.py
# @author  Michael Behrisch
# @date    2026-03-01

import datetime
import sys
from collections import defaultdict

import osm
from common import fetch_all, get_options, load_json_if_stale, save_json


DEFAULT_URL = "https://api.viz.berlin.de/FROST-Server-EcoCounter2/v1.1"


def main(args=None):
    options = get_options(args, json_default="ecocounter.json", url_default=DEFAULT_URL)
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

        # Fetch datastreams: collect all periods and directions
        datastreams = fetch_all(options.url + f"/Things({tid})/Datastreams",
                                {"$select": "@iot.id,properties,observedArea"})
        segment_id = props.get("siteID")
        if segment_id is None:
            print(f"Warning: no segment_id for {props.get('siteName', tid)}", file=sys.stderr)
            continue
        direction_siteids = {}              # direction -> siteID (for lft/rgt ordering)
        direction_coords = {}               # direction -> coords
        direction_datastreams = defaultdict(dict)  # direction -> period -> ds_id
        for ds in datastreams:
            dp = ds.get("properties", {})
            direction = dp.get("direction", "")
            if direction == "Beide":
                continue
            direction_siteids[direction] = dp.get("siteID", 0)
            direction_datastreams[direction][dp.get("periodLength", "")] = ds["@iot.id"]
            coords = (ds.get("observedArea") or {}).get("coordinates")
            if coords and direction not in direction_coords:
                direction_coords[direction] = coords

        if not direction_siteids:
            print(f"Warning: missing directional datastreams for {props.get('siteName', tid)}",
                  file=sys.stderr)
        sorted_dirs = sorted(direction_siteids, key=direction_siteids.get)
        counter = [
            {"coordinates": direction_coords.get(d), "direction": d,
             "bikes_left": i == 0, "bikes_right": i == 1,
             "datastreams": dict(sorted(direction_datastreams[d].items()))}
            for i, d in enumerate(sorted_dirs)
        ]

        # Get coordinates from Thing location for the geometry
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
            "segment_id": segment_id,
            "thing_id": tid,
            "counter": counter,
            "timezone": "Europe/Berlin",
            # preserve backup timestamps from previous run
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
