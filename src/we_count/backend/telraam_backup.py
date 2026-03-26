#!/usr/bin/env python
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    telraam_backup.py
# @author  Michael Behrisch
# @date    2023-01-03

import datetime
import json
import os
import sys

import numpy as np
import pandas as pd

import telraam_positions
import common


# original API name -> output column name (derived from COLUMN_MAP)
MODE_RENAME = {v["original"]: k for k, v in telraam_positions.COLUMN_MAP.items()}
BASIC_MODES = [v["original"] for k, v in telraam_positions.COLUMN_MAP.items() if not v.get("advanced")]
ADVANCED_MODES = [v["original"] for k, v in telraam_positions.COLUMN_MAP.items() if v.get("advanced")]
KEEP_COLUMNS = ["instance_id", "segment_id", "date", "uptime",
                "direction", "v85", "car_speed_hist_0to120plus",
                # advanced only:
                "brightness", "sharpness",
                ] + [col for m in BASIC_MODES + ADVANCED_MODES for col in (f'{m}_lft', f'{m}_rgt')]


def update_data(segments, options, conns):
    """Fetch new rows from the API. Returns (new_df, newest_data) where new_df contains
    only the newly fetched rows (not merged with existing data)."""
    print("Retrieving data for %s segments" % len(segments))
    newest_data = None
    backup_date = "last_advanced_backup" if options.advanced else "last_data_backup"
    all_new = []
    for s in segments.values():
        active = [i["first_data_package"] for i in s.get("instance_ids", {}).values() if i["first_data_package"] is not None]
        if not active:
            print("No active camera for segment %s." % s["segment_id"], file=sys.stderr)
            continue
        first = common.parse_utc(min(active) if options.clear else s.get(backup_date, min(active)))
        last = common.parse_utc(s["last_data_package"])
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
    new_df['date'] = pd.to_datetime(new_df['date'], utc=True)
    if 'car_speed_hist_0to120plus' in new_df.columns:
        scales = (new_df['car_lft'].fillna(0) + new_df['car_rgt'].fillna(0)) * new_df['uptime'].fillna(0) / 100
        new_df['car_speed_hist_0to120plus'] = [
            np.array([round(v * s) for v in hist], dtype=np.uint16) if hist is not None else None
            for hist, s in zip(new_df['car_speed_hist_0to120plus'], scales)
        ]
    count_cols = [c for c in new_df.columns if c.endswith(('_lft', '_rgt'))]
    uptime = new_df['uptime'].fillna(0)
    counts = new_df[count_cols].fillna(0).mul(uptime, axis=0).round()
    new_df[count_cols] = counts.where(counts <= 65535).astype(pd.UInt16Dtype())
    float_cols = new_df.select_dtypes(include='float64').columns
    new_df[float_cols] = new_df[float_cols].astype('float32')
    return new_df, newest_data


def _add_totals(df, modes):
    for src in modes:
        mode = MODE_RENAME.get(src, src)
        lft, rgt = f'{src}_lft', f'{src}_rgt'
        if lft not in df.columns:
            continue
        if src != mode:
            df = df.rename(columns={lft: f'{mode}_lft', rgt: f'{mode}_rgt'})
        df[f'{mode}_total'] = df[f'{mode}_lft'] + df[f'{mode}_rgt']
    return df


def _prepare_df(segments, df, advanced, month=None):
    # Filter to relevant segments and month (UTC)
    tz_map = {s['segment_id']: s.get('timezone', 'UTC') for s in segments}
    df_out = df[df['segment_id'].isin(tz_map.keys())]
    if df_out.empty:
        return None
    utc = df_out['date']
    if month is not None:
        mask = (utc.dt.year == month[0]) & (utc.dt.month == month[1])
        df_out, utc = df_out[mask], utc[mask]
        if df_out.empty:
            return None

    # Convert UTC to local time per segment and add totals
    local_ts = pd.Series(
        [dt.tz_convert(tz).strftime('%Y-%m-%d %H:%M') for dt, tz in zip(utc, df_out['segment_id'].map(tz_map))],
        index=df_out.index
    )
    df_out = df_out.assign(date_local=local_ts).drop(columns=['date'])
    # Restore uptime-corrected counts (raw counts / uptime); uptime=0 rows have count=0
    count_cols = [c for c in df_out.columns if c.endswith(('_lft', '_rgt'))]
    uptime = df_out['uptime'].where(df_out['uptime'] > 0, other=1)
    df_out[count_cols] = df_out[count_cols].div(uptime, axis=0).fillna(0).round().astype(int)
    modes = BASIC_MODES + (ADVANCED_MODES if advanced else [])
    df_out = _add_totals(df_out, modes)

    # Expand speed histogram: 25 x 5km/h bins to 8 x 10km/h bins, scale back to percentages
    hist_cols = [f'car_speed{s}' for s in range(0, 80, 10)]

    def expand_histogram(hist):
        total = 0 if hist is None else sum(hist)
        if total == 0:
            return [0.0] * 8
        result = [round((hist[2 * i] + hist[2 * i + 1]) * 100. / total, 2) for i in range(7)]
        result.append(round(sum(hist[14:]) * 100. / total, 2))
        return result

    hist_df = pd.DataFrame(
        df_out['car_speed_hist_0to120plus'].apply(expand_histogram).tolist(),
        columns=hist_cols, index=df_out.index)
    df_out = pd.concat([df_out.drop(columns=['car_speed_hist_0to120plus']), hist_df], axis=1)

    mode_cols = [f'{MODE_RENAME.get(m, m)}_{s}' for m in modes for s in ('lft', 'rgt', 'total')]
    output_cols = ['segment_id', 'date_local', 'uptime'] + mode_cols + ['v85'] + hist_cols
    return df_out[output_cols]


def _write_xl(filename, segments, df, advanced, month=None):
    df_out = _prepare_df(segments, df, advanced, month)
    if df_out is not None:
        df_out.to_excel(filename, index=False, engine='openpyxl')






def main(args=None):
    options = common.get_options(args)
    for output in (options.json_file, options.single_line_output, options.csv, options.csv_segments, options.parquet):
        if output and os.path.dirname(output):
            os.makedirs(os.path.dirname(output), exist_ok=True)
    telraam_positions.main(args)
    conns = common.ConnectionProvider(options.secrets["tokens"], options.url) if options.url else None
    segments = common.load_segments(options.json_file)

    if options.segments:
        filtered_ids = {int(s.strip()) for s in options.segments.split(",")}
        output_segments = {k: v for k, v in segments.items() if k in filtered_ids}
    else:
        output_segments = segments

    if conns:
        if options.segments:
            batches = [output_segments]
        else:
            # Sort all segments by oldest backup first, split into batches of --limit size
            backup_date = "last_advanced_backup" if options.advanced else "last_data_backup"
            sorted_segs = sorted(segments.items(), key=lambda kv: common.parse_utc_dict(kv[1], backup_date))
            batch_size = options.limit or len(sorted_segs)
            batches = [dict(sorted_segs[i:i + batch_size]) for i in range(0, len(sorted_segs), batch_size)]

        newest_data = None
        for batch in batches:
            new_df, nd = update_data(batch, options, conns)
            if nd is not None and (newest_data is None or newest_data < nd):
                newest_data = nd
            if new_df is not None:
                for year, year_new in new_df.groupby(new_df['date'].dt.year):
                    yf = common.year_file(options.parquet, year)
                    year_df = common.merge_parquet(year_new, yf)
                    year_df = year_df.sort_values(['segment_id', 'date'])
                    if os.path.exists(yf):
                        os.rename(yf, yf + ".bak")
                    year_df.to_parquet(yf, index=False, compression='zstd')
                    del year_df
                del new_df
            common.save_segments(segments, options.json_file)

        if newest_data is None:
            print("No data.", file=sys.stderr)
            return
    else:
        newest_data = datetime.datetime.now(datetime.timezone.utc)

    if options.csv:
        curr_month = (newest_data.year, newest_data.month)
        month = (options.year, 1) if options.year else common.add_month(-1, *curr_month)
        year = None
        while month <= curr_month:
            if year != month[0]:
                if year is not None:
                    del df
                year = month[0]
                df = common.load_parquet_years(options.parquet, [year])
            if options.excel:
                _write_xl(options.csv + "_%s_%02i.xlsx" % month, output_segments.values(), df, options.advanced, month)
            else:
                common.write_csv(options.csv + "_%s_%02i.csv.gz" % month, _prepare_df(output_segments.values(), df, options.advanced, month))
            month = common.add_month(1, *month)

    if options.csv_segments:
        for s in output_segments.values():
            seg_df = common.load_parquet_years(options.parquet, segments=[s['segment_id']])
            if options.excel:
                _write_xl(options.csv_segments + "_%s.xlsx" % s['segment_id'], [s], seg_df, options.advanced)
            else:
                common.write_csv(options.csv_segments + "_%s.csv.gz" % s['segment_id'], _prepare_df([s], seg_df, options.advanced))
            del seg_df

    if conns and options.verbose:
        conns.print_stats()


if __name__ == "__main__":
    main()
