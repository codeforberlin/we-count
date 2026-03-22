#!/usr/bin/env python
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    bast_backup.py
# @author  Michael Behrisch
# @date    2026-03-05

import datetime
import io
import json
import os
import re
import sys
import zipfile
from zoneinfo import ZoneInfo

import pandas as pd

import bast_positions
import common


VT_NAMES = {'Mot': 'motorcycle', 'Pkw': 'car', 'Lfw': 'delivery_van', 'PmA': 'car_trailer',
            'Bus': 'bus', 'LoA': 'rigid_truck', 'LmA': 'truck_trailer', 'Sat': 'semi_truck', 'Son': 'other'}
_TZ = ZoneInfo("Europe/Berlin")


def _parse_value(s):
    """Parse a 'NNNq' count+quality token. Returns (count, quality_char)."""
    s = s.strip()
    if len(s) < 2:
        return 0, 'a'
    try:
        return int(s[:-1]), s[-1]
    except ValueError:
        return 0, 'a'


def _parse_station_file(content):
    """Parse a BASt station data file. Returns (rows, lanes_lft, lanes_rgt)."""
    lines = content.splitlines()
    if len(lines) < 4:
        return [], 1, 0
    # R line: R{lanes_lft} {lanes_rgt} {lft_name} {lft_heading} {rgt_name} {rgt_heading};
    r_parts = lines[1][1:].split()
    try:
        lanes_lft, lanes_rgt = int(r_parts[0]), int(r_parts[1])
    except (IndexError, ValueError):
        return [], 1, 0

    # S line: S{n_groups} {n_types} {group_name_1} ... {type_name_1} ...
    # Positions 1-2: n_groups, 4-5: n_types (fixed-width)
    s_line = lines[2]
    try:
        n_groups = int(s_line[1:3])
        n_types = int(s_line[4:6])
    except (ValueError, IndexError):
        return [], ['lft']
    # Type names follow the group names in the S-line tokens
    s_tokens = s_line[1:].rstrip(';').split()
    type_names = s_tokens[2 + n_groups:2 + n_groups + n_types]
    # Map type names to output column names; skip types not in VT_NAMES
    type_cols = [(i, VT_NAMES[name]) for i, name in enumerate(type_names) if name in VT_NAMES]

    total_lanes = lanes_lft + lanes_rgt
    rows = []
    for line in lines[3:]:
        parts = line.rstrip(';\n').split()
        if len(parts) < 3:
            continue
        first = parts[0]
        if ':' in first:
            # Merged token "YYMMDDsHH:MM" — status char 's' sits between date and time
            # when the status is non-space (e.g. 'i' for inserted/extra DST hour)
            colon = first.index(':')
            date_str = first[:colon - 3]
            status = first[colon - 3]
            time_str = first[colon - 2:colon + 3]
            values = parts[1:]
        else:
            date_str, time_str, values = first, parts[1], parts[2:]
            status = ' '
        try:
            year = 2000 + int(date_str[:2])
            # Time is END of measurement interval; subtract 1 h to get start
            month, day, hour = int(date_str[2:4]), int(date_str[4:6]), int(time_str[:2]) - 1
        except (ValueError, IndexError):
            continue
        # fold=1 for 'i' (inserted) rows: in autumn DST this is the second occurrence
        # of the repeated local hour, which maps to the CET (post-transition) UTC time.
        fold = 1 if status == 'i' else 0
        utc_dt = datetime.datetime(year, month, day, hour, 0, fold=fold, tzinfo=_TZ).astimezone(datetime.timezone.utc)

        # Token layout: total_lanes * n_groups group tokens first, then
        # total_lanes * n_types individual-type tokens.
        row = {'date': utc_dt}
        for t, col in type_cols:
            for lane in range(lanes_lft):
                idx = total_lanes * n_groups + lane * n_types + t
                if idx < len(values):
                    count, q = _parse_value(values[idx])
                    row[f'{col}_lft_{lane + 1}'] = pd.NA if q == 'a' else count
                else:
                    row[f'{col}_lft_{lane + 1}'] = pd.NA
            for lane in range(lanes_rgt):
                idx = total_lanes * n_groups + (lanes_lft + lane) * n_types + t
                if idx < len(values):
                    count, q = _parse_value(values[idx])
                    row[f'{col}_rgt_{lane + 1}'] = pd.NA if q == 'a' else count
                else:
                    row[f'{col}_rgt_{lane + 1}'] = pd.NA
        rows.append(row)
    return rows, lanes_lft, lanes_rgt


def _parse_monthly_zip(zf, year, month, things, verbose=0):
    """Parse station files for our stations from an open ZipFile for one month.
    Returns DataFrame or None.
    The BASt file extension for a given year/month uses hex digits for month > 9:
    Jan=1 ... Sep=9, Oct=A, Nov=B, Dec=C."""
    ext = f"{year % 100:02d}{format(month, 'X')}"
    station_ids = set(things.keys())
    all_rows = []
    station_lanes = {}
    found = 0
    for name in zf.namelist():
        base = os.path.basename(name)
        dot = base.rfind('.')
        if dot < 0 or base[dot + 1:].upper() != ext.upper():
            continue
        m = re.match(r'[A-Za-z]+(\d+)$', base[:dot], re.IGNORECASE)
        if not m:
            continue
        sid = int(m.group(1))
        if sid not in station_ids:
            continue
        found += 1
        rows, lanes_lft, lanes_rgt = _parse_station_file(zf.read(name).decode('latin-1'))
        station_lanes[sid] = (lanes_lft, lanes_rgt)
        for row in rows:
            all_rows.append({'segment_id': sid, **row})
    if verbose:
        print(f"  {year}-{month:02d}: {found} station files, "
              f"{len(set(r['segment_id'] for r in all_rows))} with data")
    if not all_rows:
        return None, station_lanes
    df = pd.DataFrame(all_rows)
    count_cols = [c for c in df.columns if c not in ('segment_id', 'date')]
    for c in count_cols:
        df[c] = df[c].astype(pd.UInt16Dtype())
    return df, station_lanes


def _save_last_backup(json_file, backup_date, station_lanes=None):
    with open(json_file, encoding="utf8") as f:
        content = json.load(f)
    content["last_data_backup"] = backup_date.isoformat()
    if station_lanes:
        for feature in content.get("features", []):
            sid = feature["properties"]["segment_id"]
            if sid in station_lanes:
                lft, rgt = station_lanes[sid]
                feature["properties"]["lanes_lft"] = lft
                feature["properties"]["lanes_rgt"] = rgt
                feature["properties"]["directions"] = ["lft", "rgt"] if rgt > 0 else ["lft"]
    common.save_json(json_file, content)


def _prepare_df(things, df, month=None):
    tz_map = {sid: t.get("timezone", "Europe/Berlin") for sid, t in things.items()}
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
    data_cols = [c for c in df_out.columns if c not in ("segment_id", "date_local")]
    return df_out[["segment_id", "date_local"] + data_cols]


def main(args=None):
    options = common.get_options(args, json_default="bast.json",
                                 url_default=bast_positions.DEFAULT_URL,
                                 parquet_default="bast.parquet")
    if options.verbose:
        print("Fetching BASt download page...")
    annual_urls, monthly_urls = bast_positions.get_zip_urls(options.url)
    bast_positions.main(args, annual_urls, monthly_urls)
    things = common.load_segments(options.json_file)
    if not things:
        print("No station metadata found.", file=sys.stderr)
        return

    with open(options.json_file, encoding="utf8") as f:
        last_backup = common.parse_utc(json.load(f).get("last_data_backup"))

    now = datetime.datetime.now(datetime.timezone.utc)
    if options.clear or last_backup is None:
        start = (2021, 1)
    elif options.year:
        start = (options.year, 1)
    else:
        start = common.add_month(1, last_backup.year, last_backup.month)

    # Group months by their source ZIP to download each ZIP only once
    zips_to_process = {}
    m = start
    while m <= (now.year, now.month):
        year, month = m
        if year in annual_urls:
            zips_to_process.setdefault(annual_urls[year], []).append(m)
        elif m in monthly_urls:
            zips_to_process.setdefault(monthly_urls[m], []).append(m)
        else:
            if options.verbose:
                print(f"No ZIP available for {year}-{month:02d}, skipping.")
        m = common.add_month(1, *m)

    if not zips_to_process:
        print("No data sources found.", file=sys.stderr)
        return

    newest_data = None
    all_station_lanes = {}
    tmp_zip = "/tmp/bast_data.zip"
    try:
        for url, months in zips_to_process.items():
            if not bast_positions.download_zip(url, tmp_zip, options.retry, options.verbose):
                continue
            with zipfile.ZipFile(tmp_zip) as outer_zf:
                is_nested = any(n.endswith(".zip") for n in outer_zf.namelist())
                for year, month in months:
                    if is_nested:
                        inner_name = f"DZ_{year}_{month:02d}_Rohdaten.zip"
                        inner_name2 = f"DZ-{year}-{month:02d}.zip"
                        if inner_name in outer_zf.namelist():
                            inner_zf = zipfile.ZipFile(io.BytesIO(outer_zf.read(inner_name)))
                        elif inner_name2 in outer_zf.namelist():
                            inner_zf = zipfile.ZipFile(io.BytesIO(outer_zf.read(inner_name2)))
                        else:
                            if options.verbose:
                                print(f"  {inner_name} and {inner_name2} not in archive, skipping.")
                            continue
                    else:
                        inner_zf = outer_zf

                    new_df, station_lanes = _parse_monthly_zip(inner_zf, year, month, things, options.verbose)
                    all_station_lanes.update(station_lanes)
                    if is_nested:
                        inner_zf.close()

                    if new_df is not None:
                        yf = common.year_file(options.parquet, year)
                        year_df = common.merge_parquet(new_df, yf)
                        year_df = year_df.sort_values(["segment_id", "date"])
                        if os.path.exists(yf):
                            os.rename(yf, yf + ".bak")
                        year_df.to_parquet(yf, index=False, compression="zstd")
                        del year_df, new_df

                    newest_data = datetime.datetime(year, month, 1, tzinfo=datetime.timezone.utc)
                    _save_last_backup(options.json_file, newest_data, all_station_lanes)
    finally:
        if os.path.exists(tmp_zip):
            os.remove(tmp_zip)

    if newest_data is None:
        print("No data.", file=sys.stderr)
        return

    if options.csv:
        if os.path.dirname(options.csv):
            os.makedirs(os.path.dirname(options.csv), exist_ok=True)
        curr_month = (newest_data.year, newest_data.month)
        month = (options.year, 1) if options.year else common.add_month(-1, *curr_month)
        years_needed = set()
        m = month
        while m <= curr_month:
            years_needed.add(m[0])
            m = common.add_month(1, *m)
        df = common.load_parquet_years(options.parquet, years_needed)
        while month <= curr_month:
            common.write_csv(options.csv + "_%s_%02i.csv.gz" % month, _prepare_df(things, df, month))
            month = common.add_month(1, *month)


if __name__ == "__main__":
    main()
