#!/usr/bin/env python
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    bast_positions.py
# @author  Michael Behrisch
# @date    2026-03-05

import datetime
import io
import os
import re
import sys
import zipfile

from pyproj import Transformer

import common


DEFAULT_URL = "https://www.bast.de/DE/Publikationen/Daten/Verkehrstechnik/DZ.html"
REFRESH_DAYS = 30
# Non-derived vehicle type columns stored per direction (e.g. mot_1, mot_2)
COLUMN_MAP = {
    "motorcycle":    {"original": "Mot", "directions": ["lft", "rgt"]},
    "car":           {"original": "Pkw", "directions": ["lft", "rgt"]},
    "delivery_van":  {"original": "Lfw", "directions": ["lft", "rgt"]},
    "car_trailer":   {"original": "PmA", "directions": ["lft", "rgt"]},
    "bus":           {"original": "Bus", "directions": ["lft", "rgt"]},
    "rigid_truck":   {"original": "LoA", "directions": ["lft", "rgt"]},
    "truck_trailer": {"original": "LmA", "directions": ["lft", "rgt"]},
    "semi_truck":    {"original": "Sat", "directions": ["lft", "rgt"]},
    "other":         {"original": "Son", "directions": ["lft", "rgt"]},
    "heavy":         {"original": "SV",  "sum_of": ["bus", "rigid_truck", "truck_trailer", "semi_truck", "other"]},
    "motor_vehicle": {"original": "KFZ", "sum_of": ["motorcycle", "car", "delivery_van", "car_trailer", "bus", "rigid_truck", "truck_trailer", "semi_truck", "other"]},
}
# Base vehicle-type column names (without direction/lane suffixes).
# Actual parquet columns are named {type}_lft_{n} / {type}_rgt_{n} where n is the
# 1-indexed lane number, read from the S-header of each BASt station file.
DATA_COLUMNS = [k for k, v in COLUMN_MAP.items() if "sum_of" not in v]


def get_zip_urls(page_url, retries=3, retry_wait=30):
    """Fetch BASt download page. Returns (annual_urls, monthly_urls) dicts keyed by
    year (annual) or (year, month) (monthly)."""
    r = common._get_with_retry(page_url, None, retries=retries, retry_wait=retry_wait)
    if r is None:
        return {}, {}
    text = r.text
    annual = {}
    for m in re.finditer(r'files\.bast\.de/index\.php/s/(\w+)/download/DZ_(\d{4})_Rohdaten\.zip', text):
        year = int(m.group(2))
        annual[year] = f"https://{m.group()}"
    monthly = {}
    for m in re.finditer(r'files\.bast\.de/index\.php/s/(\w+)/download/DZ_(\d{4})_(\d{2})_Rohdaten\.zip', text):
        year, month = int(m.group(2)), int(m.group(3))
        monthly[(year, month)] = f"https://{m.group()}"
    return annual, monthly


def download_zip(url, target, retries=1, retry_wait=60, verbose=0):
    """Stream-download a ZIP to target path."""
    if verbose:
        print(f"Downloading {url}")
    r = common._get_with_retry(url, None, retries=retries, retry_wait=retry_wait, stream=True)
    if r is None:
        return False
    with open(target, "wb") as f:
        for chunk in r.iter_content(chunk_size=65536):
            f.write(chunk)
    return True


def _parse_metadata(zip_path, bbox):
    """Parse station metadata CSV from a BASt ZIP, filter by bbox. Returns GeoJSON features list."""
    if bbox:
        west, south, east, north = [float(x) for x in bbox.split(",")]
    transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)
    features = []
    with zipfile.ZipFile(zip_path) as zf:
        meta_name = next((n for n in zf.namelist() if n.endswith("_Metadaten.csv")), None)
        if not meta_name:
            print("No metadata CSV found in ZIP.", file=sys.stderr)
            return []
        content = zf.read(meta_name).decode("latin-1").replace("ß", "ss")
        lines = content.splitlines()
        col = {name.strip(): i for i, name in enumerate(lines[0].split(";"))}
        for line in lines[1:]:
            if not line.strip():
                continue
            fields = line.split(";")
            if len(fields) <= max(col.values()):
                continue
            try:
                easting = float(fields[col["Koordinaten_UTM32_E"]].replace(",", "."))
                northing = float(fields[col["Koordinaten_UTM32_N"]].replace(",", "."))
            except (ValueError, KeyError):
                continue
            lon, lat = transformer.transform(easting, northing)
            if bbox and not (west <= lon <= east and south <= lat <= north):
                continue
            sid = int(fields[col["Dauerzaehlstellennummer"]])
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [round(lon, 6), round(lat, 6)]},
                "properties": {
                    "segment_id": sid,
                    "name": fields[col["Dauerzaehlstellenname"]].strip(),
                    "state": fields[col["Landeskuerzel"]].strip(),
                    "road_class": fields[col["Strassenklasse"]].strip(),
                    "road_number": fields[col["Strassennummer"]].strip(),
                    "direction_lft": fields[col["Nahziel_Richtung_1"]].strip(),
                    "heading_lft": fields[col["Himmelsrichtung_Richtung_1"]].strip(),
                    "direction_rgt": fields[col["Nahziel_Richtung_2"]].strip(),
                    "heading_rgt": fields[col["Himmelsrichtung_Richtung_2"]].strip(),
                    "timezone": "Europe/Berlin",
                    "last_data_backup": None,
                    "last_advanced_backup": None,
                },
            })
    return features


def main(args=None, annual_urls=None, monthly_urls=None):
    options = common.get_options(args, json_default="bast.json", url_default=DEFAULT_URL, year_default=2021)

    if not options.clear and os.path.exists(options.json_file):
        import json
        with open(options.json_file, encoding="utf8") as f:
            existing = json.load(f)
        age = datetime.datetime.now(datetime.timezone.utc) - common.parse_utc_dict(existing.get("properties", {}), "created_at")
        if age < datetime.timedelta(days=REFRESH_DAYS):
            if options.verbose:
                print(f"{options.json_file} is less than {REFRESH_DAYS} days old, skipping.")
            return False

    if annual_urls is None or monthly_urls is None:
        if options.verbose:
            print("Fetching BASt download page for metadata...")
        annual_urls, monthly_urls = get_zip_urls(options.url)

    # Use the most recent available ZIP for station metadata
    now = datetime.datetime.now()
    zip_url = None
    for year in range(now.year, options.year - 1, -1):
        if year in annual_urls:
            zip_url = annual_urls[year]
            break
        for month in range(12, 0, -1):
            if (year, month) in monthly_urls:
                zip_url = monthly_urls[(year, month)]
                break
        if zip_url:
            break
    if not zip_url:
        print("Could not find any BASt ZIP URL.", file=sys.stderr)
        return False

    tmp_zip = "/tmp/bast_meta.zip"
    if not download_zip(zip_url, tmp_zip, options.retry, verbose=options.verbose):
        return False
    # Annual ZIPs are ZIP-of-ZIPs; extract the first inner ZIP for metadata
    with zipfile.ZipFile(tmp_zip) as zf:
        inner_names = [n for n in zf.namelist() if n.endswith(".zip")]
        if inner_names:
            inner_data = io.BytesIO(zf.read(inner_names[0]))
            with zipfile.ZipFile(inner_data) as inner_zf:
                tmp_inner = "/tmp/bast_meta_inner.zip"
                with open(tmp_inner, "wb") as f:
                    inner_data.seek(0)
                    f.write(inner_data.read())
            features = _parse_metadata(tmp_inner, options.bbox)
            os.remove(tmp_inner)
        else:
            features = _parse_metadata(tmp_zip, options.bbox)

    if not features:
        print("No stations found in bounding box.", file=sys.stderr)
        return False

    # Preserve backup timestamps from existing data
    old_things = common.load_segments(options.json_file)
    for f in features:
        sid = f["properties"]["segment_id"]
        if sid in old_things:
            for key in ("last_data_backup", "last_advanced_backup"):
                f["properties"][key] = old_things[sid].get(key)

    if options.verbose:
        print(f"Saving {len(features)} stations to {options.json_file}")

    if os.path.dirname(options.json_file):
        os.makedirs(os.path.dirname(options.json_file), exist_ok=True)
    common.save_json(options.json_file, {
        "type": "FeatureCollection",
        "properties": {
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "columns": DATA_COLUMNS,
            "column_map": COLUMN_MAP,
        },
        "features": features,
    })
    return True


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
