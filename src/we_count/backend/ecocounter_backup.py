#!/usr/bin/env python
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    ecocounter_backup.py
# @author  Michael Behrisch
# @date    2026-02-28

import datetime
import os
import sys

import pandas as pd

import ecocounter_positions
import common

PERIOD_NORMAL = "1-Stunde"
PERIOD_ADVANCED = "15-Min"


def _fetch_observations(url, datastream_id, since, retries=1):
    """Fetch all observations for a datastream since a given UTC datetime."""
    filter_str = f"phenomenonTime ge {since.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
    obs = common.fetch_all(
        url + f"/Datastreams({datastream_id})/Observations",
        {"$select": "phenomenonTime,result", "$filter": filter_str,
         "$orderby": "phenomenonTime asc", "$top": 1000},
        retries=retries
    )
    return {o["phenomenonTime"].split("/")[0]: o["result"] for o in obs}


def update_data(things, options):
    """Fetch new observations for all things. Returns (new_df, newest_data) where
    new_df contains only the newly fetched rows."""
    print(f"Retrieving data for {len(things)} stations")
    backup_date = "last_advanced_backup" if options.advanced else "last_data_backup"
    period = PERIOD_ADVANCED if options.advanced else PERIOD_NORMAL
    newest_data = None
    all_new = []
    for sid, t in things.items():
        counters = t.get("counter", [])
        ds_lft = next((c["datastreams"].get(period) for c in counters if c.get("bikes_left")), None)
        ds_rgt = next((c["datastreams"].get(period) for c in counters if c.get("bikes_right")), None)
        first_data = common.parse_utc(t.get("firstData", ""))
        since = first_data if options.clear else (common.parse_utc_dict(t, backup_date) or first_data)
        if options.verbose:
            print(f"Fetching {t['siteName']} since {since}")
        obs_lft = _fetch_observations(options.url, ds_lft, since, options.retry) if ds_lft else {}
        obs_rgt = _fetch_observations(options.url, ds_rgt, since, options.retry) if ds_rgt else {}
        all_dates = sorted(set(obs_lft) | set(obs_rgt))
        if all_dates:
            new_df = pd.DataFrame({
                "segment_id": sid,
                "date": pd.to_datetime(all_dates, utc=True, format="ISO8601"),
                "bike_lft": (pd.array([obs_lft.get(d, 0) for d in all_dates], dtype=pd.UInt16Dtype())
                             if ds_lft else pd.array([pd.NA] * len(all_dates), dtype=pd.UInt16Dtype())),
                "bike_rgt": (pd.array([obs_rgt.get(d, 0) for d in all_dates], dtype=pd.UInt16Dtype())
                             if ds_rgt else pd.array([pd.NA] * len(all_dates), dtype=pd.UInt16Dtype())),
            })
            all_new.append(new_df)
            last = common.parse_utc(all_dates[-1])  # all_dates are ISO strings from FROST
            if newest_data is None or newest_data < last:
                newest_data = last
        t[backup_date] = datetime.datetime.now(datetime.timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0).isoformat()
    return pd.concat(all_new, ignore_index=True) if all_new else None, newest_data


def _prepare_df(things, df, month=None):
    """Filter, timezone-convert, and select output columns."""
    tz_map = {tid: t.get("timezone", "Europe/Berlin") for tid, t in things.items()}
    df_out = df[df["segment_id"].isin(tz_map.keys())]
    if df_out.empty:
        return None
    utc = df_out["date"]
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
    return df_out[["segment_id", "date_local", "bike_lft", "bike_rgt"]]


def main(args=None):
    options = common.get_options(args, json_default="ecocounter.json",
                          url_default=ecocounter_positions.DEFAULT_URL, parquet_default="ecocounter.parquet")
    ecocounter_positions.main(args)
    things = common.load_segments(options.json_file)
    if not things:
        print("No station metadata found.", file=sys.stderr)
        return
    backup_date = "last_advanced_backup" if options.advanced else "last_data_backup"
    sorted_things = sorted(things.items(), key=lambda kv: common.parse_utc_dict(kv[1], backup_date))
    batch_size = options.limit or len(sorted_things)
    batches = [dict(sorted_things[i:i + batch_size]) for i in range(0, len(sorted_things), batch_size)]

    newest_data = None
    for batch in batches:
        new_df, nd = update_data(batch, options)
        if nd is not None and (newest_data is None or newest_data < nd):
            newest_data = nd
        if new_df is not None:
            merged = common.merge_parquet(new_df, options.parquet)
            merged = merged.sort_values(["segment_id", "date"])
            if os.path.exists(options.parquet):
                os.rename(options.parquet, options.parquet + ".bak")
            merged.to_parquet(options.parquet, index=False, compression="zstd")
            del merged, new_df
        common.save_segments(batch, options.json_file)

    if newest_data is None:
        print("No data.", file=sys.stderr)
        return
    if options.csv:
        if os.path.dirname(options.csv):
            os.makedirs(os.path.dirname(options.csv), exist_ok=True)
        df = pd.read_parquet(options.parquet)
        curr_month = (newest_data.year, newest_data.month)
        month = (options.year, 1) if options.year else common.add_month(-1, *curr_month)
        while month <= curr_month:
            common.write_csv(options.csv + "_%s_%02i.csv.gz" % month, _prepare_df(things, df, month))
            month = common.add_month(1, *month)
    if options.csv_segments:
        if os.path.dirname(options.csv_segments):
            os.makedirs(os.path.dirname(options.csv_segments), exist_ok=True)
        df = pd.read_parquet(options.parquet)
        for i, s in things.items():
            common.write_csv(options.csv_segments + "_%s.csv.gz" % s['segment_id'], _prepare_df({i:s}, df))


if __name__ == "__main__":
    main()
