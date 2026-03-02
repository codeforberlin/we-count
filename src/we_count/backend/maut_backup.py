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
import common


def load_things(json_file):
    if not os.path.exists(json_file):
        return {}, None
    with open(json_file, encoding="utf8") as f:
        data = json.load(f)
    things = {f["properties"]["segment_id"]: f["properties"]
              for f in data.get("features", [])}
    last_backup = common.parse_utc(data.get("last_data_backup"))
    return things, last_backup


def save_last_backup(json_file, backup_date):
    with open(json_file, encoding="utf8") as f:
        content = json.load(f)
    content["last_data_backup"] = backup_date.isoformat()
    common.save_json(json_file, content)


def _fetch_raw(segment_ids, options, since):
    """Fetch all section observations since 'since' for the bounding box.
    Returns (list_of_row_dicts, newest_datetime) — the list keeps all segments
    in memory so callers can batch-process by segment_id."""
    layer = options.url + "/abschnitte_view/FeatureServer/0"
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")
    if options.verbose:
        print(f"Fetching observations since {since_str}")
    raw = common.fetch_arcgis_features(layer, {
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
    newest = common.parse_utc(max(r["date"] for r in rows))
    return rows, newest


def _year_file(parquet, year):
    base = parquet[:-len(".parquet")] if parquet.endswith(".parquet") else parquet
    return f"{base}_{year}.parquet"


def _merge_year(new_year_df, year_file):
    if not os.path.exists(year_file):
        return new_year_df
    existing = pd.read_parquet(year_file)
    new_keys = new_year_df.set_index(["segment_id", "date"]).index
    keep = ~existing.set_index(["segment_id", "date"]).index.isin(new_keys)
    return pd.concat([existing[keep], new_year_df], ignore_index=True)


def load_parquet_years(parquet, years):
    parts = [pd.read_parquet(_year_file(parquet, y))
             for y in years if os.path.exists(_year_file(parquet, y))]
    if not parts:
        if os.path.exists(parquet):
            df = pd.read_parquet(parquet)
            return df[df["date"].str[:4].isin([str(y) for y in years])]
        return None
    return pd.concat(parts, ignore_index=True)


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
        [dt.tz_convert(tz).strftime("%Y-%m-%d") for dt, tz in zip(utc, df_out["segment_id"].map(tz_map))],
        index=df_out.index
    )
    df_out = df_out.assign(date_local=local_ts).drop(columns=["date"])
    return df_out[["segment_id", "date_local", "lkw"]]


def _write_csv(filename, things, df, month=None):
    df_out = _prepare_df(things, df, month)
    if df_out is None:
        return
    with gzip.open(filename, "wt") as csv_file:
        df_out.to_csv(csv_file, index=False)


def main(args=None):
    options = common.get_options(args, json_default="maut.json",
                          url_default=maut_positions.DEFAULT_URL, parquet_default="maut.parquet")
    maut_positions.main(args)
    things, last_backup = load_things(options.json_file)
    if not things:
        print("No section metadata found.", file=sys.stderr)
        return
    epoch = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
    since = epoch if options.clear else (last_backup or epoch)
    # Single bulk API call — all segment data arrives at once
    raw_rows, newest_data = _fetch_raw(set(things.keys()), options, since)
    if raw_rows is None:
        print("No data.", file=sys.stderr)
        return
    # Merge into year-split parquet files in batches of --limit segments
    all_ids = sorted(things.keys())
    batch_size = options.limit or len(all_ids)
    for i in range(0, len(all_ids), batch_size):
        batch_ids = set(all_ids[i:i + batch_size])
        batch_rows = [r for r in raw_rows if r["segment_id"] in batch_ids]
        if not batch_rows:
            continue
        new_df = pd.DataFrame(batch_rows)
        new_df["lkw"] = new_df["lkw"].astype("uint32")
        for year, year_new in new_df.groupby(new_df["date"].str[:4]):
            yf = _year_file(options.parquet, year)
            year_df = _merge_year(year_new, yf)
            year_df = year_df.sort_values(["segment_id", "date"])
            if os.path.exists(yf):
                os.rename(yf, yf + ".bak")
            year_df.to_parquet(yf, index=False, compression="zstd")
            del year_df
        del new_df
    del raw_rows
    save_last_backup(options.json_file, newest_data)
    if options.csv:
        if os.path.dirname(options.csv):
            os.makedirs(os.path.dirname(options.csv), exist_ok=True)
        curr_month = (newest_data.year, newest_data.month)
        month = (options.csv_start_year, 1) if options.csv_start_year else common.add_month(-1, *curr_month)
        years_needed = set()
        m = month
        while m <= curr_month:
            years_needed.add(m[0])
            m = common.add_month(1, *m)
        df = load_parquet_years(options.parquet, years_needed)
        while month <= curr_month:
            _write_csv(options.csv + "_%s_%02i.csv.gz" % month, things, df, month)
            month = common.add_month(1, *month)


if __name__ == "__main__":
    main()
