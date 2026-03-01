#!/usr/bin/env python
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    teu_backup.py
# @author  Michael Behrisch
# @date    2026-03-01

import datetime
import gzip
import json
import os
import sys

import pandas as pd

import teu_positions
from common import fetch_all, get_options, parse_utc, parse_utc_dict, add_month
from teu_positions import DEFAULT_URL

PERIOD_NORMAL = "1-Stunde"
PERIOD_ADVANCED = "5-Min"
VEHICLES = ["KFZ", "PKW", "LKW"]


def load_things(json_file):
    if not os.path.exists(json_file):
        return {}
    with open(json_file, encoding="utf8") as f:
        data = json.load(f)
    return {f["properties"]["segment_id"]: f["properties"]
            for f in data.get("features", [])}


def save_things(things, json_file):
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
    filter_str = f"phenomenonTime ge {since.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
    obs = fetch_all(
        url + f"/Datastreams({datastream_id})/Observations",
        {"$select": "phenomenonTime,result", "$filter": filter_str,
         "$orderby": "phenomenonTime asc", "$top": 1000}
    )
    return {o["phenomenonTime"].split("/")[0]: o["result"] for o in obs}


def update_data(things, options):
    print(f"Retrieving data for {len(things)} stations")
    backup_date = "last_advanced_backup" if options.advanced else "last_data_backup"
    period = PERIOD_ADVANCED if options.advanced else PERIOD_NORMAL
    newest_data = None
    all_new = []
    for sid, t in things.items():
        ds_by_vehicle = {v: t.get("datastreams", {}).get(v, {}).get("Anzahl", {}).get(period)
                         for v in VEHICLES}
        if not any(ds_by_vehicle.values()):
            print(f"Missing datastreams for {t.get('name', sid)}, skipping.", file=sys.stderr)
            continue
        epoch = datetime.datetime(2010, 1, 1, tzinfo=datetime.timezone.utc)
        since = epoch if options.clear else (parse_utc_dict(t, backup_date) or epoch)
        if options.verbose:
            print(f"Fetching {t.get('name', sid)} since {since}")
        obs_by_vehicle = {v: _fetch_observations(options.url, ds_id, since)
                          for v, ds_id in ds_by_vehicle.items() if ds_id}
        all_dates = sorted(set().union(*obs_by_vehicle.values()))
        if not all_dates:
            t[backup_date] = datetime.datetime.now(datetime.timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0).isoformat()
            continue
        row = {"segment_id": sid, "date": all_dates}
        for v in VEHICLES:
            obs = obs_by_vehicle.get(v, {})
            row[v.lower()] = pd.array([obs.get(d, 0) for d in all_dates], dtype="uint16")
        all_new.append(pd.DataFrame(row))
        last = parse_utc(all_dates[-1])
        if newest_data is None or newest_data < last:
            newest_data = last
        t[backup_date] = datetime.datetime.now(datetime.timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0).isoformat()
    return pd.concat(all_new, ignore_index=True) if all_new else None, newest_data


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
    df_out = df_out.assign(date_local=local_ts.dt.strftime("%Y-%m-%d %H:%M")).drop(columns=["date"])
    count_cols = [v.lower() for v in VEHICLES]
    return df_out[["segment_id", "date_local"] + count_cols]


def _write_csv(filename, things, df, month=None):
    df_out = _prepare_df(things, df, month)
    if df_out is None:
        return
    with gzip.open(filename, "wt") as csv_file:
        df_out.to_csv(csv_file, index=False)


def main(args=None):
    options = get_options(args, json_default="teu.json",
                          url_default=DEFAULT_URL, parquet_default="teu.parquet")
    if os.path.exists(options.parquet):
        df = pd.read_parquet(options.parquet)
        count_cols = [v.lower() for v in VEHICLES if v.lower() in df.columns]
        df[count_cols] = df[count_cols].astype("uint16")
    else:
        df = None
    teu_positions.main(args)
    things = load_things(options.json_file)
    if not things:
        print("No station metadata found.", file=sys.stderr)
        return
    if options.limit:
        backup_date = "last_advanced_backup" if options.advanced else "last_data_backup"
        epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        sorted_things = sorted(things.items(), key=lambda kv: parse_utc_dict(kv[1], backup_date) or epoch)
        things = dict(sorted_things[:options.limit])
    new_df, newest_data = update_data(things, options)
    if new_df is None:
        print("No data.", file=sys.stderr)
        return
    if df is not None:
        for sid, sid_new in new_df.groupby("segment_id"):
            df = df[~((df["segment_id"] == sid) & df["date"].isin(sid_new["date"]))]
        df = pd.concat([df, new_df], ignore_index=True)
    else:
        df = new_df
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
