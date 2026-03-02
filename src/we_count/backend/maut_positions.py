#!/usr/bin/env python
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    maut_positions.py
# @author  Michael Behrisch
# @date    2026-03-01

import datetime
import sys

import requests

import osm
import common


DEFAULT_URL = "https://webgis.toll-collect.de/server/rest/services/Hosted"


def _esri_polyline_to_geojson(geom):
    paths = (geom or {}).get("paths", [])
    if not paths:
        return None
    if len(paths) == 1:
        return {"type": "LineString", "coordinates": paths[0]}
    return {"type": "MultiLineString", "coordinates": paths}


def main(args=None):
    options = common.get_options(args, json_default="maut.json", url_default=DEFAULT_URL)
    old_features = common.load_json_if_stale(options.json_file, options.clear, options.verbose)
    if old_features is None:
        return False
    old_data = {f["properties"]["segment_id"]: f["properties"] for f in old_features}

    layer = options.url + "/abschnitte_view/FeatureServer/0"

    # Find the most recent available date
    r = requests.get(layer + "/query", params={
        "where": "1=1", "outFields": "datum", "returnGeometry": "false",
        "orderByFields": "datum DESC", "resultRecordCount": "1", "f": "json",
    })
    r.raise_for_status()
    latest_ms = r.json()["features"][0]["attributes"]["datum"]
    latest_dt = datetime.datetime.fromtimestamp(latest_ms / 1000, tz=datetime.timezone.utc)
    latest_str = latest_dt.strftime("%Y-%m-%d %H:%M:%S")
    if options.verbose:
        print(f"Fetching sections for {latest_str}")

    raw = common.fetch_arcgis_features(layer, {
        "where": f"datum=timestamp '{latest_str}'",
        "geometry": options.bbox,
        "inSR": "4326",
        "geometryType": "esriGeometryEnvelope",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "abschnitt_id,bundesfernstrasse,laenge_km,strassen_typ,"
                     "mautknoten_name_von,mautknoten_name_nach",
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "json",
    })
    if options.verbose:
        print(f"{len(raw)} sections found.")
    if not raw:
        print("No sections found for the given bounding box.", file=sys.stderr)
        return False

    features = []
    for f in raw:
        attrs = f["attributes"]
        segment_id = attrs["abschnitt_id"]
        existing = old_data.get(segment_id, {})
        feature_props = {
            "segment_id": segment_id,
            "bundesfernstrasse": attrs.get("bundesfernstrasse"),
            "laenge_km": attrs.get("laenge_km"),
            "strassen_typ": attrs.get("strassen_typ"),
            "mautknoten_name_von": attrs.get("mautknoten_name_von"),
            "mautknoten_name_nach": attrs.get("mautknoten_name_nach"),
            "timezone": "Europe/Berlin",
            "last_data_backup": existing.get("last_data_backup"),
        }
        features.append({
            "type": "Feature",
            "geometry": _esri_polyline_to_geojson(f.get("geometry")),
            "properties": feature_props,
        })

    osm.add_osm(features, old_data)
    common.save_json(options.json_file, {
        "type": "FeatureCollection",
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "features": features,
    })
    return True


if __name__ == "__main__":
    main()
