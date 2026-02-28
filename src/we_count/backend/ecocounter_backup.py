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
import requests

from common import get_options, parse_utc, parse_utc_dict, add_month


DEFAULT_URL = "https://api.viz.berlin.de/FROST-Server-EcoCounter2/v1.1"
PERIOD_NORMAL = "1-Stunde"
PERIOD_ADVANCED = "15-Min"


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


def load_things(json_file):
    """Load things metadata from JSON file. Returns dict keyed by int thing_id."""
    if not os.path.exists(json_file):
        return {}
    with open(json_file, encoding="utf8") as f:
        data = json.load(f)
    return {int(k): v for k, v in data.get("things", {}).items()}


def save_things(things, json_file):
    """Write updated things dict to JSON file atomically."""
    data = {
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "things": {str(k): v for k, v in things.items()}
    }
    tmp = json_file + ".new"
    with open(tmp, "w", encoding="utf8") as f:
        json.dump(data, f, indent=2)
    os.rename(tmp, json_file)


def update_things(things, url):
    """Fetch all Things from FROST API and cache their Datastream IDs."""
    print("Updating Things metadata from API...")
    all_things = fetch_all(url + "/Things", {"$select": "@iot.id,name,properties"})
    for thing in all_things:
        tid = thing["@iot.id"]
        props = thing.get("properties", {})
        # Fetch datastreams for this Thing
        datastreams = fetch_all(
            url + f"/Things({tid})/Datastreams",
            {"$select": "@iot.id,properties"}
        )
        ds_lft = ds_rgt = ds_lft_adv = ds_rgt_adv = None
        for ds in datastreams:
            dp = ds.get("properties", {})
            site_id = dp.get("siteID", 0)
            period = dp.get("periodLength", "")
            prefix = site_id // 1000000 if site_id else 0
            if prefix == 101:
                if period == PERIOD_NORMAL:
                    ds_lft = ds["@iot.id"]
                elif period == PERIOD_ADVANCED:
                    ds_lft_adv = ds["@iot.id"]
            elif prefix == 102:
                if period == PERIOD_NORMAL:
                    ds_rgt = ds["@iot.id"]
                elif period == PERIOD_ADVANCED:
                    ds_rgt_adv = ds["@iot.id"]
        existing = things.get(tid, {})
        first_data = props.get("firstData", "")
        if first_data and "T" not in first_data:
            first_data += "T00:00:00Z"
        things[tid] = {
            "siteName": props.get("siteName", thing.get("name", "")),
            "district": props.get("district", ""),
            "timezone": "Europe/Berlin",
            "firstData": first_data,
            "datastream_lft": ds_lft,
            "datastream_rgt": ds_rgt,
            "datastream_lft_advanced": ds_lft_adv,
            "datastream_rgt_advanced": ds_rgt_adv,
            # preserve backup timestamps
            "last_data_backup": existing.get("last_data_backup"),
            "last_advanced_backup": existing.get("last_advanced_backup"),
        }
    return things


def _fetch_observations(url, datastream_id, since):
    """Fetch all observations for a datastream since a given UTC datetime."""
    filter_str = f"phenomenonTime ge {since.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
    obs = fetch_all(
        url + f"/Datastreams({datastream_id})/Observations",
        {"$select": "phenomenonTime,result", "$filter": filter_str,
         "$orderby": "phenomenonTime asc", "$top": 1000}
    )
    return {o["phenomenonTime"].split("/")[0]: o["result"] for o in obs}


def update_data(things, df, options):
    """Fetch new observations for all things, merge into df."""
    print(f"Retrieving data for {len(things)} stations")
    backup_date = "last_advanced_backup" if options.advanced else "last_data_backup"
    newest_data = None
    for tid, t in things.items():
        ds_lft = t.get("datastream_lft_advanced" if options.advanced else "datastream_lft")
        ds_rgt = t.get("datastream_rgt_advanced" if options.advanced else "datastream_rgt")
        if not ds_lft or not ds_rgt:
            print(f"Missing datastreams for station {t['siteName']}, skipping.", file=sys.stderr)
            continue
        epoch = datetime.datetime(2010, 1, 1, tzinfo=datetime.timezone.utc)
        first_data = parse_utc(t.get("firstData", "")) or epoch
        since = first_data if options.clear else (parse_utc_dict(t, backup_date) or first_data)
        if options.verbose:
            print(f"Fetching {t['siteName']} since {since}")
        obs_lft = _fetch_observations(options.url, ds_lft, since)
        obs_rgt = _fetch_observations(options.url, ds_rgt, since)
        all_dates = sorted(set(obs_lft) | set(obs_rgt))
        if not all_dates:
            t[backup_date] = datetime.datetime.now(datetime.timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0).isoformat()
            continue
        new_df = pd.DataFrame({
            "segment_id": tid,
            "date": all_dates,
            "bike_lft": pd.array([obs_lft.get(d, 0) for d in all_dates], dtype="uint16"),
            "bike_rgt": pd.array([obs_rgt.get(d, 0) for d in all_dates], dtype="uint16"),
        })
        new_df["bike_total"] = (new_df["bike_lft"] + new_df["bike_rgt"]).astype("uint16")
        if df is None:
            df = new_df
        else:
            df = pd.concat([df[~df["date"].isin(new_df["date"]) | (df["segment_id"] != tid)],
                            new_df], ignore_index=True)
        last = parse_utc(all_dates[-1])
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
        [dt.tz_convert(tz) for dt, tz in zip(utc, df_out["segment_id"].map(tz_map))],
        index=df_out.index
    )
    df_out = df_out.assign(date_local=local_ts.dt.strftime("%Y-%m-%d %H:%M")).drop(columns=["date"])
    return df_out[["segment_id", "date_local", "bike_lft", "bike_rgt", "bike_total"]]


def _write_csv(filename, things, df, month=None):
    df_out = _prepare_df(things, df, month)
    if df_out is None:
        return
    with gzip.open(filename, "wt") as csv_file:
        df_out.to_csv(csv_file, index=False)


def main(args=None):
    options = get_options(args, json_default="ecocounter.json",
                          url_default=DEFAULT_URL, parquet_default="ecocounter.parquet")
    if os.path.exists(options.parquet):
        df = pd.read_parquet(options.parquet)
        count_cols = [c for c in df.columns if c.endswith(("_lft", "_rgt", "_total"))]
        df[count_cols] = df[count_cols].astype("uint16")
    else:
        df = None
    things = load_things(options.json_file)
    # Refresh Things metadata if missing or stale (>1 day old)
    needs_update = not things
    if not needs_update and os.path.exists(options.json_file):
        with open(options.json_file, encoding="utf8") as f:
            meta = json.load(f)
        age = datetime.datetime.now(datetime.timezone.utc) - parse_utc(meta.get("created_at", "1970-01-01"))
        needs_update = age > datetime.timedelta(days=1)
    if needs_update:
        things = update_things(things, options.url)
        save_things(things, options.json_file)
    if options.limit:
        backup_date = "last_advanced_backup" if options.advanced else "last_data_backup"
        epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        sorted_things = sorted(things.items(), key=lambda kv: parse_utc_dict(kv[1], backup_date) or epoch)
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
        month = (options.csv_start_year, 1) if options.csv_start_year else add_month(-1, *curr_month)
        while month <= curr_month:
            _write_csv(options.csv + "_%s_%02i.csv.gz" % month, things, df, month)
            month = add_month(1, *month)


if __name__ == "__main__":
    main()
