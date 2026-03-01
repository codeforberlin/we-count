#!/usr/bin/env python
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    maut_backup.py
# @author  Michael Behrisch
# @date    2026-03-01

import datetime
import gzip
import json
import os
import sys

import pandas as pd

import maut_positions
from common import fetch_arcgis_features, get_options, parse_utc, add_month, save_json
from maut_positions import DEFAULT_URL


def load_things(json_file):
    if not os.path.exists(json_file):
        return {}, None
    with open(json_file, encoding="utf8") as f:
        data = json.load(f)
    things = {f["properties"]["segment_id"]: f["properties"]
              for f in data.get("features", [])}
    last_backup = parse_utc(data.get("last_data_backup"))
    return things, last_backup


def save_last_backup(json_file, backup_date):
    with open(json_file, encoding="utf8") as f:
        content = json.load(f)
    content["last_data_backup"] = backup_date.isoformat()
    save_json(json_file, content)


def update_data(segment_ids, options, since):
    """Fetch all section observations since 'since' for the bounding box."""
    layer = options.url + "/abschnitte_view/FeatureServer/0"
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")
    if options.verbose:
        print(f"Fetching observations since {since_str}")
    raw = fetch_arcgis_features(layer, {
        "where": f"datum>timestamp '{since_str}'",
        "geometry": options.bbox,
        "inSR": "4326",
        "geometryType": "esriGeometryEnvelope",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "abschnitt_id,datum,anzahl_befahrungen",
        "returnGeometry": "false",
        "f": "json",
    })
    if not raw:
        return None, None
    rows = []
    for f in raw:
        a = f["attributes"]
        if a["abschnitt_id"] not in segment_ids:
            continue
        dt = datetime.datetime.fromtimestamp(a["datum"] / 1000, tz=datetime.timezone.utc)
        rows.append({
            "segment_id": a["abschnitt_id"],
            "date": dt.isoformat(),
            "lkw": a["anzahl_befahrungen"] or 0,
        })
    if not rows:
        return None, None
    new_df = pd.DataFrame(rows)
    new_df["lkw"] = new_df["lkw"].astype("uint32")
    newest = parse_utc(max(new_df["date"]))
    return new_df, newest


def _prepare_df(things, df, month=None):
    tz_map = {sid: t.get("timezone", "Europe/Berlin") for sid, t in things.items()}
    df_out = df[df["segment_id"].isin(tz_map.keys())]
    if df_out.empty:
        return None
    utc = pd.to_datetime(df_out["date"], utc=True)
    if month is not None:
        mask = (utc.dt.year == month[0]) & (utc.dt.month == month[1])
        df_out, utc = df_out[mask], utc[mask]
        if df_out.empty:
            return None
    local_ts = pd.Series(
        [dt.tz_convert(tz) for dt, tz in zip(utc, df_out["segment_id"].map(tz_map))],
        index=df_out.index
    )
    df_out = df_out.assign(date_local=local_ts.dt.strftime("%Y-%m-%d")).drop(columns=["date"])
    return df_out[["segment_id", "date_local", "lkw"]]


def _write_csv(filename, things, df, month=None):
    df_out = _prepare_df(things, df, month)
    if df_out is None:
        return
    with gzip.open(filename, "wt") as csv_file:
        df_out.to_csv(csv_file, index=False)


def main(args=None):
    options = get_options(args, json_default="maut.json",
                          url_default=DEFAULT_URL, parquet_default="maut.parquet")
    if os.path.exists(options.parquet):
        df = pd.read_parquet(options.parquet)
    else:
        df = None
    maut_positions.main(args)
    things, last_backup = load_things(options.json_file)
    if not things:
        print("No section metadata found.", file=sys.stderr)
        return
    epoch = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
    since = epoch if options.clear else (last_backup or epoch)
    new_df, newest_data = update_data(set(things.keys()), options, since)
    if new_df is None:
        print("No data.", file=sys.stderr)
        return
    if df is not None:
        new_keys = new_df.set_index(["segment_id", "date"]).index
        keep = ~df.set_index(["segment_id", "date"]).index.isin(new_keys)
        df = pd.concat([df[keep], new_df], ignore_index=True)
    else:
        df = new_df
    df = df.sort_values(["segment_id", "date"])
    df.to_parquet(options.parquet, index=False, compression="zstd")
    save_last_backup(options.json_file, newest_data)
    if options.csv:
        if os.path.dirname(options.csv):
            os.makedirs(os.path.dirname(options.csv), exist_ok=True)
        curr_month = (newest_data.year, newest_data.month)
        month = (options.csv_start_year, 1) if options.csv_start_year else add_month(-1, *curr_month)
        while month <= curr_month:
            _write_csv(options.csv + "_%s_%02i.csv.gz" % month, things, df, month)
            month = add_month(1, *month)


if __name__ == "__main__":
    main()
