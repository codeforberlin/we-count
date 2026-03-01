#!/usr/bin/env python
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    ecocounter_positions.py
# @author  Michael Behrisch
# @date    2026-03-01

import datetime
import json
import os
import sys
from collections import defaultdict

import requests

from common import get_options, parse_utc_dict


DEFAULT_URL = "https://api.viz.berlin.de/FROST-Server-EcoCounter2/v1.1"


def fetch_all(url, params=None):
    """Paginated GET — follows @iot.nextLink until exhausted."""
    result = []
    while url:
        r = requests.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        result.extend(data.get("value", []))
        url = data.get("@iot.nextLink")
        params = None  # params are encoded in nextLink
    return result


def main(args=None):
    options = get_options(args, json_default="ecocounter.json", url_default=DEFAULT_URL)
    old_data = {}
    if os.path.exists(options.json_file) and not options.clear:
        with open(options.json_file, encoding="utf8") as f:
            old_json = json.load(f)
        last_mod = parse_utc_dict(old_json, "created_at")
        delta = datetime.timedelta(minutes=30)
        if datetime.datetime.now(datetime.timezone.utc) - last_mod < delta:
            if options.verbose:
                print(f"Not recreating {options.json_file}, it is less than {delta} old.")
            return False
        old_data = {f["properties"]["segment_id"]: f["properties"]
                    for f in old_json.get("features", [])}

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

    result = {
        "type": "FeatureCollection",
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "features": features,
    }
    with open(options.json_file + ".new", "w", encoding="utf8") as f:
        json.dump(result, f, indent=2)
    os.rename(options.json_file + ".new", options.json_file)
    return True


if __name__ == "__main__":
    main()
