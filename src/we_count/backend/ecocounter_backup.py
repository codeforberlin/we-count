#!/usr/bin/env python
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    ecocounter_backup.py
# @author  Michael Behrisch
# @date    2026-02-28

import datetime
import gzip
import json
import os
import sys

import pandas as pd

import ecocounter_positions
import common

PERIOD_NORMAL = "1-Stunde"
PERIOD_ADVANCED = "15-Min"


def load_things(json_file):
    """Load things from GeoJSON file. Returns dict keyed by segment_id (siteID)."""
    if not os.path.exists(json_file):
        return {}
    with open(json_file, encoding="utf8") as f:
        data = json.load(f)
    return {f["properties"]["segment_id"]: f["properties"]
            for f in data.get("features", [])}


def save_things(things, json_file):
    """Write updated backup timestamps back into the GeoJSON file."""
    with open(json_file, encoding="utf8") as f:
        content = json.load(f)
    for feature in content.get("features", []):
        sid = feature["properties"]["segment_id"]
        if sid in things:
            feature["properties"] = things[sid]
    with open(json_file + ".new", "w", encoding="utf8") as f:
        json.dump(content, f, indent=2)
    os.rename(json_file + ".new", json_file)


def _fetch_observations(url, datastream_id, since):
    """Fetch all observations for a datastream since a given UTC datetime."""
    filter_str = f"phenomenonTime ge {since.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
    obs = common.fetch_all(
        url + f"/Datastreams({datastream_id})/Observations",
        {"$select": "phenomenonTime,result", "$filter": filter_str,
         "$orderby": "phenomenonTime asc", "$top": 1000}
    )
    return {o["phenomenonTime"].split("/")[0]: o["result"] for o in obs}


def update_data(things, df, options):
    """Fetch new observations for all things, merge into df."""
    print(f"Retrieving data for {len(things)} stations")
    backup_date = "last_advanced_backup" if options.advanced else "last_data_backup"
    period = PERIOD_ADVANCED if options.advanced else PERIOD_NORMAL
    newest_data = None
    for sid, t in things.items():
        counters = t.get("counter", [])
        ds_lft = next((c["datastreams"].get(period) for c in counters if c.get("bikes_left")), None)
        ds_rgt = next((c["datastreams"].get(period) for c in counters if c.get("bikes_right")), None)
        if not ds_lft:
            print(f"Missing datastreams for station {t['siteName']}, skipping.", file=sys.stderr)
            continue
        epoch = datetime.datetime(2010, 1, 1, tzinfo=datetime.timezone.utc)
        first_data = common.parse_utc(t.get("firstData", "")) or epoch
        since = first_data if options.clear else (common.parse_utc_dict(t, backup_date) or first_data)
        if options.verbose:
            print(f"Fetching {t['siteName']} since {since}")
        obs_lft = _fetch_observations(options.url, ds_lft, since)
        obs_rgt = _fetch_observations(options.url, ds_rgt, since) if ds_rgt else {}
        all_dates = sorted(set(obs_lft) | set(obs_rgt))
        if not all_dates:
            t[backup_date] = datetime.datetime.now(datetime.timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0).isoformat()
            continue
        new_df = pd.DataFrame({
            "segment_id": sid,
            "date": all_dates,
            "bike_lft": pd.array([obs_lft.get(d, 0) for d in all_dates], dtype="uint16"),
            "bike_rgt": pd.array([obs_rgt.get(d, 0) for d in all_dates], dtype="uint16"),
        })
        new_df["bike_total"] = (new_df["bike_lft"] + new_df["bike_rgt"]).astype("uint16")
        if df is None:
            df = new_df
        else:
            df = pd.concat([df[~df["date"].isin(new_df["date"]) | (df["segment_id"] != sid)],
                            new_df], ignore_index=True)
        last = common.parse_utc(all_dates[-1])
        if newest_data is None or newest_data < last:
            newest_data = last
        t[backup_date] = datetime.datetime.now(datetime.timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0).isoformat()
    return df, newest_data


def _prepare_df(things, df, month=None):
    """Filter, timezone-convert, and select output columns."""
    tz_map = {tid: t.get("timezone", "Europe/Berlin") for tid, t in things.items()}
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
        [dt.tz_convert(tz).strftime("%Y-%m-%d %H:%M") for dt, tz in zip(utc, df_out["segment_id"].map(tz_map))],
        index=df_out.index
    )
    df_out = df_out.assign(date_local=local_ts).drop(columns=["date"])
    return df_out[["segment_id", "date_local", "bike_lft", "bike_rgt", "bike_total"]]


def _write_csv(filename, things, df, month=None):
    df_out = _prepare_df(things, df, month)
    if df_out is None:
        return
    with gzip.open(filename, "wt") as csv_file:
        df_out.to_csv(csv_file, index=False)


def main(args=None):
    options = common.get_options(args, json_default="ecocounter.json",
                          url_default=ecocounter_positions.DEFAULT_URL, parquet_default="ecocounter.parquet")
    if os.path.exists(options.parquet):
        df = pd.read_parquet(options.parquet)
        count_cols = [c for c in df.columns if c.endswith(("_lft", "_rgt", "_total"))]
        df[count_cols] = df[count_cols].astype("uint16")
    else:
        df = None
    ecocounter_positions.main(args)
    things = load_things(options.json_file)
    if not things:
        print("No station metadata found.", file=sys.stderr)
        return
    if options.limit:
        backup_date = "last_advanced_backup" if options.advanced else "last_data_backup"
        epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        sorted_things = sorted(things.items(), key=lambda kv: common.parse_utc_dict(kv[1], backup_date) or epoch)
        things = dict(sorted_things[:options.limit])
    df, newest_data = update_data(things, df, options)
    if df is None:
        print("No data.", file=sys.stderr)
        return
    df = df.sort_values(["segment_id", "date"])
    df.to_parquet(options.parquet, index=False, compression="zstd")
    save_things(load_things(options.json_file) | things, options.json_file)
    if options.csv:
        if os.path.dirname(options.csv):
            os.makedirs(os.path.dirname(options.csv), exist_ok=True)
        curr_month = (newest_data.year, newest_data.month)
        month = (options.csv_start_year, 1) if options.csv_start_year else common.add_month(-1, *curr_month)
        while month <= curr_month:
            _write_csv(options.csv + "_%s_%02i.csv.gz" % month, things, df, month)
            month = common.add_month(1, *month)


if __name__ == "__main__":
    main()
