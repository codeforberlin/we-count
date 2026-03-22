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


VEHICLE_TYPES = ['KFZ', 'SV', 'Mot', 'Pkw', 'Lfw', 'PmA', 'Bus', 'LoA', 'LmA', 'Sat', 'Son']
# KFZ = sum of all, SV = sum of heavy vehicles — both are derived and not stored
DERIVED_TYPES = {'KFZ', 'SV'}
VT_NAMES = {'Mot': 'motorcycle', 'Pkw': 'car', 'Lfw': 'delivery_van', 'PmA': 'car_trailer',
            'Bus': 'bus', 'LoA': 'rigid_truck', 'LmA': 'truck_trailer', 'Sat': 'semi_truck', 'Son': 'other'}
NON_DERIVED = [(i, vt) for i, vt in enumerate(VEHICLE_TYPES) if vt not in DERIVED_TYPES]
DATA_COLUMNS = bast_positions.DATA_COLUMNS
N_VT = len(VEHICLE_TYPES)
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
    """Parse a BASt station data file. Returns (rows, directions) where directions is
    ['lft', 'rgt'] for bidirectional stations or ['lft'] for unidirectional ones."""
    lines = content.splitlines()
    if len(lines) < 4:
        return [], ['lft']
    # R line: R{lanes_lft} {lanes_rgt} {lft_name} {lft_heading} {rgt_name} {rgt_heading};
    r_parts = lines[1][1:].split()
    try:
        lanes_lft, lanes_rgt = int(r_parts[0]), int(r_parts[1])
    except (IndexError, ValueError):
        return [], ['lft']

    directions = ['lft', 'rgt'] if lanes_rgt > 0 else ['lft']
    rows = []
    for line in lines[3:]:
        parts = line.rstrip(';\n').split()
        if len(parts) < 3:
            continue
        first = parts[0]
        if ':' in first:
            # Merged token "YYMMDDqHH:MM" — quality char sits between date and time
            colon = first.index(':')
            date_str = first[:colon - 3]
            time_str = first[colon - 2:colon + 3]
            values = parts[1:]
        else:
            date_str, time_str, values = first, parts[1], parts[2:]
        try:
            year = 2000 + int(date_str[:2])
            # Time is END of measurement interval; subtract 1 h to get start
            month, day, hour = int(date_str[2:4]), int(date_str[4:6]), int(time_str[:2]) - 1
        except (ValueError, IndexError):
            continue
        utc_dt = datetime.datetime(year, month, day, hour, 0, tzinfo=_TZ).astimezone(datetime.timezone.utc)

        def lane_sum(type_idx, lane_offset, n_lanes):
            total = 0
            for lane in range(n_lanes):
                idx = (lane_offset + lane) * N_VT + type_idx
                if idx < len(values):
                    count, q = _parse_value(values[idx])
                    if q != 'a':
                        total += count
            return total

        row = {'date': utc_dt}
        for type_idx, vt in NON_DERIVED:
            col = VT_NAMES[vt]
            row[f'{col}_lft'] = lane_sum(type_idx, 0, lanes_lft)
            row[f'{col}_rgt'] = lane_sum(type_idx, lanes_lft, lanes_rgt) if lanes_rgt > 0 else pd.NA
        rows.append(row)
    return rows, directions


def _parse_monthly_zip(zf, year, month, things, verbose=0):
    """Parse station files for our stations from an open ZipFile for one month.
    Returns DataFrame or None.
    The BASt file extension for a given year/month uses hex digits for month > 9:
    Jan=1 ... Sep=9, Oct=A, Nov=B, Dec=C."""
    ext = f"{year % 100:02d}{format(month, 'X')}"
    station_ids = set(things.keys())
    all_rows = []
    station_directions = {}
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
        rows, directions = _parse_station_file(zf.read(name).decode('latin-1'))
        station_directions[sid] = directions
        for row in rows:
            all_rows.append({'segment_id': sid, **row})
    if verbose:
        print(f"  {year}-{month:02d}: {found} station files, "
              f"{len(set(r['segment_id'] for r in all_rows))} with data")
    if not all_rows:
        return None, station_directions
    df = pd.DataFrame(all_rows)
    for full in DATA_COLUMNS:
        if full in df.columns:
            df[full] = df[full].astype(pd.Int32Dtype())
    return df, station_directions


def _save_last_backup(json_file, backup_date, station_directions=None):
    with open(json_file, encoding="utf8") as f:
        content = json.load(f)
    content["last_data_backup"] = backup_date.isoformat()
    if station_directions:
        for feature in content.get("features", []):
            sid = feature["properties"]["segment_id"]
            if sid in station_directions:
                feature["properties"]["directions"] = station_directions[sid]
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
    return df_out[["segment_id", "date_local"] + list(DATA_COLUMNS)]


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
    all_station_directions = {}
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
                        if inner_name not in outer_zf.namelist():
                            if options.verbose:
                                print(f"  {inner_name} not in archive, skipping.")
                            continue
                        inner_zf = zipfile.ZipFile(io.BytesIO(outer_zf.read(inner_name)))
                    else:
                        inner_zf = outer_zf

                    new_df, station_directions = _parse_monthly_zip(inner_zf, year, month, things, options.verbose)
                    all_station_directions.update(station_directions)
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
                    _save_last_backup(options.json_file, newest_data, all_station_directions)
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
