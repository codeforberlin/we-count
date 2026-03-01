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

from common import ConnectionProvider, get_options, add_month, parse_utc, parse_utc_dict


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
    with open(json_file + ".new", "w", encoding="utf8") as segment_file:
        json.dump(content, segment_file, indent=2)
    os.rename(json_file + ".new", json_file)


def update_data(segments, df: pd.DataFrame, options, conns):
    print("Retrieving data for %s segments" % len(segments))
    newest_data = None
    backup_date = "last_advanced_backup" if options.advanced else "last_data_backup"
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
        new_rows = []
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
                    new_rows.append(batch[[c for c in KEEP_COLUMNS if c in batch.columns]].dropna(axis=1, how='all'))
            else:
                error = True
                break
            first = interval_end
        if error:
            continue
        if new_rows:
            new_df = pd.concat(new_rows, ignore_index=True)
            del new_rows
            if 'car_speed_hist_0to120plus' in new_df.columns:
                scales = (new_df['car_lft'] + new_df['car_rgt']) * new_df['uptime'] / 100
                new_df['car_speed_hist_0to120plus'] = [
                    np.array([round(v * s) for v in hist], dtype=np.uint16) if hist is not None else None
                    for hist, s in zip(new_df['car_speed_hist_0to120plus'], scales)
                ]
            count_cols = [c for c in new_df.columns if c.endswith(('_lft', '_rgt'))]
            new_df[count_cols] = new_df[count_cols].fillna(0).round().astype('uint16')
            float_cols = new_df.select_dtypes(include='float64').columns
            new_df[float_cols] = new_df[float_cols].astype('float32')
            if df is None:
                df = new_df
            else:
                df = pd.concat([df[~df["date"].isin(new_df["date"]) | (df["segment_id"] != s["segment_id"])],
                                new_df], ignore_index=True)
            del new_df
        s[backup_date] = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        if last is not None and (newest_data is None or newest_data < last):
            newest_data = last
    return df, newest_data


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


def main(args=None):
    options = get_options(args)
    df = pd.read_parquet(options.parquet) if os.path.exists(options.parquet) else None
    conns = ConnectionProvider(options.secrets["tokens"], options.url) if options.url else None
    excel = False
    segments = load_segments(options.json_file)
    if options.segments:
        filtered = [int(s.strip()) for s in options.segments.split(",")]
        filtered_segments = {k:v for k,v in segments.items() if k in filtered}
    elif options.limit:
        backup_date = "last_advanced_backup" if options.advanced else "last_data_backup"
        backup_times = sorted([(parse_utc_dict(v, backup_date), k) for k,v in segments.items()])[:options.limit]
        filtered_segments = {k:v for k,v in segments.items() if k in list(zip(*backup_times))[1]}
    else:
        filtered_segments = segments
    if conns:
        df, newest_data = update_data(filtered_segments, df, options, conns)
        if df is None:
            print("No data.", file=sys.stderr)
            return
        df = df.sort_values(['segment_id', 'date'])
        df.to_parquet(options.parquet, index=False, compression='zstd')
    else:
        newest_data = datetime.datetime.now(datetime.timezone.utc)
    save_segments(segments, options.json_file)
    if options.csv:
        if os.path.dirname(options.csv):
            os.makedirs(os.path.dirname(options.csv), exist_ok=True)
        curr_month = (newest_data.year, newest_data.month)
        month = (options.csv_start_year, 1) if options.csv_start_year else add_month(-1, *curr_month)
        while month <= curr_month:
            if excel:
                _write_xl(options.csv + "_%s_%02i.xlsx" % month, filtered_segments.values(), df, options.advanced, month)
            else:
                _write_csv(options.csv + "_%s_%02i.csv.gz" % month, filtered_segments.values(), df, options.advanced, month)
            month = add_month(1, *month)

    if options.csv_segments:
        if os.path.dirname(options.csv_segments):
            os.makedirs(os.path.dirname(options.csv_segments), exist_ok=True)
        for s in filtered_segments.values():
            if excel:
                _write_xl(options.csv_segments + "_%s.xlsx" % s.id, [s], options.advanced)
            else:
                _write_csv(options.csv_segments + "_%s.csv.gz" % s.id, [s], options.advanced)

    if conns and options.verbose:
        conns.print_stats()


if __name__ == "__main__":
    main()
