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

import argparse
import datetime
import json
import os
import sys

import requests

import common


WFS_BASE = "https://gdi.berlin.de/services/wfs/verkehrsmengen_{year}"
REFRESH_DAYS = 30


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
    # Pre-parse --year so we can set the right json default before get_options.
    # Pass remaining_args to get_options so --year doesn't cause an error there.
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--year", type=int, default=2023)
    pre_args, remaining_args = pre.parse_known_args(args)
    year = pre_args.year

    options = common.get_options(remaining_args, json_default=f"vmk_{year}.json")
    options.year = year
    wfs_url = WFS_BASE.format(year=year)

    # Skip if file is fresh enough
    if not options.clear and os.path.exists(options.json_file):
        with open(options.json_file, encoding="utf8") as f:
            existing = json.load(f)
        age = datetime.datetime.now(datetime.timezone.utc) - common.parse_utc(existing.get("created_at", "1970-01-01"))
        if age < datetime.timedelta(days=REFRESH_DAYS):
            if options.verbose:
                print(f"{options.json_file} is less than {REFRESH_DAYS} days old, skipping.")
            return False

    if options.verbose:
        print(f"Fetching WFS layers for {year}...")
    kfz_features = _fetch_layer(wfs_url, f"verkehrsmengen_{year}:dtvw{year}kfz", options.verbose)
    lkw_features = _fetch_layer(wfs_url, f"verkehrsmengen_{year}:dtvw{year}lkw", options.verbose)
    rad_features = _fetch_layer(wfs_url, f"verkehrsmengen_{year}:dtvw{year}rad", options.verbose)
    if not kfz_features:
        print(f"No KFZ features found for year {year}. Is this year available?", file=sys.stderr)
        return False

    # Build lookup dicts for the count values and geometries
    lkw_by_link = {f["properties"]["link_id"]: f["properties"]["dtvw_lkw"] for f in lkw_features}
    rad_by_link  = {f["properties"]["link_id"]: f["properties"]["dtvw_rad"]  for f in rad_features}
    rad_feat_by_link = {f["properties"]["link_id"]: f for f in rad_features}

    def _props(raw, extra):
        p = raw["properties"]
        return {
            "segment_id": p["link_id"],
            "str_name":   p.get("str_name"),
            "str_bez":    p.get("str_bez"),
            "bezirk":     p.get("bezirk"),
            "stadtteil":  p.get("stadtteil"),
            "strklasse":  p.get("strklasse"),
            "strklasse1": p.get("strklasse1"),
            "strklasse2": p.get("strklasse2"),
            **extra,
        }

    # KFZ layer is the primary source (geometry + kfz/lkw/rad counts)
    kfz_link_ids = set()
    geo_features = []
    for f in kfz_features:
        link_id = f["properties"]["link_id"]
        kfz_link_ids.add(link_id)
        geo_features.append({
            "type": "Feature",
            "geometry": f["geometry"],
            "properties": _props(f, {
                "dtvw_kfz": f["properties"].get("dtvw_kfz"),
                "dtvw_lkw": lkw_by_link.get(link_id),
                "dtvw_rad": rad_by_link.get(link_id),
            }),
        })

    # Rad-only edges (cycle paths not in KFZ layer)
    for link_id, f in rad_feat_by_link.items():
        if link_id in kfz_link_ids:
            continue
        geo_features.append({
            "type": "Feature",
            "geometry": f["geometry"],
            "properties": _props(f, {
                "dtvw_kfz": None,
                "dtvw_lkw": None,
                "dtvw_rad": f["properties"].get("dtvw_rad"),
            }),
        })

    if options.verbose:
        print(f"Saving {len(geo_features)} features ({len(kfz_link_ids)} KFZ/LKW + "
              f"{len(geo_features) - len(kfz_link_ids)} Rad-only) to {options.json_file}")

    if os.path.dirname(options.json_file):
        os.makedirs(os.path.dirname(options.json_file), exist_ok=True)

    common.save_json(options.json_file, {
        "type": "FeatureCollection",
        "year": year,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "features": geo_features,
    })
    return True


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
