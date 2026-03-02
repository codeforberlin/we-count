#!/usr/bin/env python
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    backup_data.py
# @author  Michael Behrisch
# @date    2023-01-03

import datetime
import gzip
import json
import os
import sys

import numpy as np
import pandas as pd

import sensor_positions
from common import ConnectionProvider, get_options, add_month, parse_utc, parse_utc_dict, save_json


BASIC_MODES = ['pedestrian', 'bike', 'car', 'heavy']
ADVANCED_MODES = ['mode_bus', 'mode_lighttruck', 'mode_motorcycle', 'mode_stroller',
                  'mode_tractor', 'mode_trailer', 'mode_truck', 'mode_night']
KEEP_COLUMNS = ["instance_id", "segment_id", "date", "uptime",
                "direction", "v85", "car_speed_hist_0to120plus",
                # advanced only:
                "brightness", "sharpness",
                ] + [col for m in BASIC_MODES + ADVANCED_MODES for col in (f'{m}_lft', f'{m}_rgt')]


def load_segments(json_file):
    segments = {}
    with open(json_file, encoding="utf8") as segment_file:
        for segment in json.load(segment_file).get("features", []):
            sid = segment["properties"]["segment_id"]
            segments[sid] = segment["properties"]
    return segments


def save_segments(segments, json_file):
    with open(json_file, encoding="utf8") as segment_file:
        content = json.load(segment_file)
    for segment in content.get("features", []):
        sid = segment["properties"]["segment_id"]
        if sid in segments:
            segment["properties"] = segments[sid]
    save_json(json_file, content)


def update_data(segments, options, conns):
    """Fetch new rows from the API. Returns (new_df, newest_data) where new_df contains
    only the newly fetched rows (not merged with existing data)."""
    print("Retrieving data for %s segments" % len(segments))
    newest_data = None
    backup_date = "last_advanced_backup" if options.advanced else "last_data_backup"
    all_new = []
    for s in segments.values():
        active = [i["first_data_package"] for i in s["instance_ids"].values() if i["first_data_package"] is not None]
        if not active:
            print("No active camera for segment %s." % s["segment_id"], file=sys.stderr)
            continue
        first = parse_utc(min(active) if options.clear else s.get(backup_date, min(active)))
        last = parse_utc(s["last_data_package"])
        if options.verbose and last is not None and first < last:
            print("Retrieving data for segment %s between %s and %s." % (s["segment_id"], first, last))
        error = False
        while last is not None and first < last:
            interval_end = first + datetime.timedelta(days=20)
            payload = {
                "level": "segments", "format": "per-hour", "id": s["segment_id"],
                "time_start": first.isoformat(), "time_end": interval_end.isoformat()}
            if options.advanced:
                payload.update(format="per-quarter", columns=",".join(KEEP_COLUMNS))
                res = conns.request("/advanced/reports/traffic", "POST", str(payload), options.retry, "report")
                if res.get("status_code") == 403:
                    print(" Skipping %s." % s["segment_id"], file=sys.stderr)
                    error = True
                    break
            else:
                res = conns.request("/v1/reports/traffic", "POST", str(payload), options.retry, "report")
            if options.dump:
                with open(options.dump, "a", encoding="utf8") as dump:
                    json.dump(res, dump, indent=2)
            if "report" in res:
                if res["report"]:
                    batch = pd.DataFrame(res["report"])
                    all_new.append(batch[[c for c in KEEP_COLUMNS if c in batch.columns]].dropna(axis=1, how='all'))
            else:
                error = True
                break
            first = interval_end
        if error:
            continue
        s[backup_date] = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        if last is not None and (newest_data is None or newest_data < last):
            newest_data = last
    if not all_new:
        return None, newest_data
    new_df = pd.concat(all_new, ignore_index=True)
    if 'car_speed_hist_0to120plus' in new_df.columns:
        scales = (new_df['car_lft'].fillna(0) + new_df['car_rgt'].fillna(0)) * new_df['uptime'].fillna(0) / 100
        new_df['car_speed_hist_0to120plus'] = [
            np.array([round(v * s) for v in hist], dtype=np.uint16) if hist is not None else None
            for hist, s in zip(new_df['car_speed_hist_0to120plus'], scales)
        ]
    count_cols = [c for c in new_df.columns if c.endswith(('_lft', '_rgt'))]
    new_df[count_cols] = new_df[count_cols].fillna(0).round().astype('uint16')
    float_cols = new_df.select_dtypes(include='float64').columns
    new_df[float_cols] = new_df[float_cols].astype('float32')
    return new_df, newest_data


def _add_totals(df, modes):
    for src in modes:
        mode = 'ped' if src == 'pedestrian' else src
        lft, rgt = f'{src}_lft', f'{src}_rgt'
        if lft not in df.columns:
            continue
        if src != mode:
            df = df.rename(columns={lft: f'{mode}_lft', rgt: f'{mode}_rgt'})
        df[f'{mode}_total'] = (df[f'{mode}_lft'] + df[f'{mode}_rgt']).astype('uint16')
    return df


def _prepare_df(segments, df, advanced, month):
    # Filter to relevant segments and month (UTC)
    tz_map = {s['segment_id']: s.get('timezone', 'UTC') for s in segments}
    df_out = df[df['segment_id'].isin(tz_map.keys())]
    if df_out.empty:
        return None
    utc = pd.to_datetime(df_out['date'], utc=True)
    if month is not None:
        mask = (utc.dt.year == month[0]) & (utc.dt.month == month[1])
        df_out, utc = df_out[mask], utc[mask]
        if df_out.empty:
            return None

    # Convert UTC to local time per segment and add totals
    local_ts = pd.Series(
        [dt.tz_convert(tz) for dt, tz in zip(utc, df_out['segment_id'].map(tz_map))],
        index=df_out.index
    )
    df_out = df_out.assign(date_local=local_ts.dt.strftime('%Y-%m-%d %H:%M')).drop(columns=['date'])
    modes = BASIC_MODES + (ADVANCED_MODES if advanced else [])
    df_out = _add_totals(df_out, modes)

    # Expand speed histogram: 25 x 5km/h bins to 8 x 10km/h bins, scale back to percentages
    hist_cols = [f'car_speed{s}' for s in range(0, 80, 10)]

    def expand_histogram(hist):
        total = sum(hist or [0])
        if total == 0:
            return [0.0] * 8
        result = [round((hist[2 * i] + hist[2 * i + 1]) * 100 / total, 2) for i in range(7)]
        result.append(round(sum(hist[14:]) * 100 / total, 2))
        return result

    hist_df = pd.DataFrame(
        df_out['car_speed_hist_0to120plus'].apply(expand_histogram).tolist(),
        columns=hist_cols, index=df_out.index)
    df_out = pd.concat([df_out.drop(columns=['car_speed_hist_0to120plus']), hist_df], axis=1)

    mode_cols = [f'{"ped" if m == "pedestrian" else m}_{s}' for m in modes for s in ('lft', 'rgt', 'total')]
    output_cols = ['segment_id', 'date_local', 'uptime'] + mode_cols + ['v85'] + hist_cols
    return df_out[output_cols]


def _write_xl(filename, segments, df, advanced, month=None):
    df_out = _prepare_df(segments, df, advanced, month)
    if df_out is not None:
        df_out.to_excel(filename, index=False, engine='openpyxl')


def _write_csv(filename, segments, df, advanced, month=None, delimiter=","):
    df_out = _prepare_df(segments, df, advanced, month)
    if df_out is None:
        return
    with gzip.open(filename, 'wt') as csv_file:
        df_out.to_csv(csv_file, index=False, sep=delimiter)


def _year_file(parquet, year):
    base = parquet[:-len(".parquet")] if parquet.endswith(".parquet") else parquet
    return f"{base}_{year}.parquet"


def _merge_year(new_year_df, year_file):
    """Merge new rows for one year with the existing year parquet, return merged df."""
    if not os.path.exists(year_file):
        return new_year_df
    existing = pd.read_parquet(year_file)
    # Drop existing rows whose (segment_id, date) will be replaced by new data
    new_keys = new_year_df.set_index(['segment_id', 'date']).index
    keep = ~existing.set_index(['segment_id', 'date']).index.isin(new_keys)
    return pd.concat([existing[keep], new_year_df], ignore_index=True)


def load_parquet_years(parquet, years):
    """Load only the requested year parquet files. Falls back to single file."""
    parts = []
    for year in years:
        yf = _year_file(parquet, year)
        if os.path.exists(yf):
            parts.append(pd.read_parquet(yf))
    if not parts:
        # backward compat: single file
        if os.path.exists(parquet):
            df = pd.read_parquet(parquet)
            return df[df['date'].str[:4].isin([str(y) for y in years])]
        return None
    return pd.concat(parts, ignore_index=True)


def main(args=None):
    options = get_options(args)
    sensor_positions.main(args)
    conns = ConnectionProvider(options.secrets["tokens"], options.url) if options.url else None
    excel = False
    segments = load_segments(options.json_file)

    # Segments used for CSV output (may be a subset when --segments is given)
    if options.segments:
        filtered_ids = {int(s.strip()) for s in options.segments.split(",")}
        output_segments = {k: v for k, v in segments.items() if k in filtered_ids}
    else:
        output_segments = segments

    if conns:
        if options.segments:
            # Explicit segment list → single batch
            batches = [output_segments]
        else:
            # Sort all segments by oldest backup first, split into batches of --limit size
            backup_date = "last_advanced_backup" if options.advanced else "last_data_backup"
            sorted_segs = sorted(segments.items(), key=lambda kv: parse_utc_dict(kv[1], backup_date))
            batch_size = options.limit or len(sorted_segs)
            batches = [dict(sorted_segs[i:i + batch_size]) for i in range(0, len(sorted_segs), batch_size)]

        newest_data = None
        for batch in batches:
            new_df, nd = update_data(batch, options, conns)
            if nd is not None and (newest_data is None or newest_data < nd):
                newest_data = nd
            if new_df is not None:
                for year, year_new in new_df.groupby(new_df['date'].str[:4]):
                    yf = _year_file(options.parquet, year)
                    year_df = _merge_year(year_new, yf)
                    year_df = year_df.sort_values(['segment_id', 'date'])
                    if os.path.exists(yf):
                        os.rename(yf, yf + ".bak")
                    year_df.to_parquet(yf, index=False, compression='zstd')
                    del year_df
                del new_df
            save_segments(segments, options.json_file)

        if newest_data is None:
            print("No data.", file=sys.stderr)
            return
    else:
        newest_data = datetime.datetime.now(datetime.timezone.utc)

    if options.csv:
        if os.path.dirname(options.csv):
            os.makedirs(os.path.dirname(options.csv), exist_ok=True)
        curr_month = (newest_data.year, newest_data.month)
        month = (options.csv_start_year, 1) if options.csv_start_year else add_month(-1, *curr_month)
        df = load_parquet_years(options.parquet, range(month[0], curr_month[0] + 1))
        while month <= curr_month:
            if excel:
                _write_xl(options.csv + "_%s_%02i.xlsx" % month, output_segments.values(), df, options.advanced, month)
            else:
                _write_csv(options.csv + "_%s_%02i.csv.gz" % month, output_segments.values(), df, options.advanced, month)
            month = add_month(1, *month)

    if options.csv_segments:
        if os.path.dirname(options.csv_segments):
            os.makedirs(os.path.dirname(options.csv_segments), exist_ok=True)
        for s in output_segments.values():
            if excel:
                _write_xl(options.csv_segments + "_%s.xlsx" % s.id, [s], df, options.advanced)
            else:
                _write_csv(options.csv_segments + "_%s.csv.gz" % s.id, [s], df, options.advanced)

    if conns and options.verbose:
        conns.print_stats()


if __name__ == "__main__":
    main()
