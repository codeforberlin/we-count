#!/usr/bin/env python
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    vmk_import.py
# @author  Michael Behrisch
# @date    2026-03-01
#
# Imports the Berlin Verkehrsmengenkarte (annual average traffic counts)
# from the GDI Berlin WFS service into a GeoJSON file.
# Covers ~8,800 road edges across all Berlin street types.
# Data: DTVw (Durchschnittlicher täglicher Verkehr Werktag)
# Available years: 2019, 2023 (others return 404)

import datetime
import json
import os
import sys

import requests

import common


WFS_BASE = "https://gdi.berlin.de/services/wfs/verkehrsmengen_{year}"
REFRESH_DAYS = 30
COLUMN_MAP = {
    "car":           {"original": "derived: dtvw_kfz - dtvw_lkw"},
    "heavy":         {"original": "dtvw_lkw"},
    "bike":          {"original": "dtvw_rad"},
    "motor_vehicle": {"original": "dtvw_kfz", "sum_of": ["car", "heavy"]},
}
DATA_COLUMNS = list(COLUMN_MAP.keys())


def _fetch_layer(wfs_url, layer_name, verbose=0):
    """Fetch all features from a WFS 2.0 layer with startIndex pagination.
    Returns empty list if the layer does not exist (404 / OGC exception)."""
    features = []
    page = 1000
    start = 0
    while True:
        r = requests.get(wfs_url, params={
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": layer_name, "count": page, "startIndex": start,
            "srsName": "EPSG:4326", "outputFormat": "application/json",
        })
        if r.status_code in (400, 404):
            return []
        r.raise_for_status()
        data = r.json()
        # OGC exception (layer not found) comes back as JSON with "exceptions"
        if "exceptions" in data:
            return []
        batch = data.get("features", [])
        features.extend(batch)
        if verbose and start == 0:
            print(f"  {layer_name}: {data.get('numberMatched', '?')} total features")
        if len(batch) < page:
            break
        start += len(batch)
    return features


def main(args=None):
    options = common.get_options(args, json_default=f"vmk_2023.json", year_default=2023)
    year = options.year
    wfs_url = WFS_BASE.format(year=year)

    # Skip if file is fresh enough
    if not options.clear and os.path.exists(options.json_file):
        with open(options.json_file, encoding="utf8") as f:
            existing = json.load(f)
        age = datetime.datetime.now(datetime.timezone.utc) - common.parse_utc_dict(existing.get("properties", {}), "created_at")
        if age < datetime.timedelta(days=REFRESH_DAYS):
            if options.verbose:
                print(f"{options.json_file} is less than {REFRESH_DAYS} days old, skipping.")
            return False

    if options.verbose:
        print(f"Fetching WFS layers for {year}...")

    layers = [
        (f"verkehrsmengen_{year}:dtvw{year}kfz", "dtvw_kfz", "motor_vehicle"),
        (f"verkehrsmengen_{year}:dtvw{year}lkw", "dtvw_lkw", "heavy"),
        (f"verkehrsmengen_{year}:dtvw{year}rad", "dtvw_rad", "bike"),
    ]
    features_by_link = {}
    for layer_name, prop_name, translation in layers:
        fetched = _fetch_layer(wfs_url, layer_name, options.verbose)
        for f in fetched:
            link_id = f["properties"]["link_id"]
            if link_id not in features_by_link:
                features_by_link[link_id] = {
                    "type": "Feature",
                    "geometry": f["geometry"],
                    "properties": {"segment_id": link_id, **f["properties"]},
                }
            features_by_link[link_id]["properties"][translation] = f["properties"].get(prop_name)
    if not features_by_link:
        print(f"No features found for year {year}. Is this year available?", file=sys.stderr)
        return False

    # Derive car = motor_vehicle - heavy
    for f in features_by_link.values():
        p = f["properties"]
        mv = p.pop("motor_vehicle", None)
        heavy = p.get("heavy")
        if mv is not None and heavy is not None:
            p["car"] = mv - heavy

    geo_features = list(features_by_link.values())
    if options.verbose:
        car_count = sum(1 for f in geo_features if f["properties"].get("car") is not None)
        print(f"Saving {len(geo_features)} features ({car_count} with motor vehicle data + "
              f"{len(geo_features) - car_count} bike-only) to {options.json_file}")

    if os.path.dirname(options.json_file):
        os.makedirs(os.path.dirname(options.json_file), exist_ok=True)

    common.save_json(options.json_file, {
        "type": "FeatureCollection",
        "properties": {
            "year": year,
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "columns": DATA_COLUMNS,
            "column_map": COLUMN_MAP,
        },
        "features": geo_features,
    })
    return True


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
