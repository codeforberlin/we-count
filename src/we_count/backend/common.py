# Copyright (c) 2023-2024 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    common.py
# @author  Michael Behrisch
# @date    2023-01-15

import argparse
import datetime
import glob
import gzip
import json
import os
import pprint
import random
import sys
import time

import pandas as pd
import requests


GEO_JSON_NAME = "bzm_telraam_segments.geojson"


class ConnectionProvider:
    def __init__(self, tokens, url):
        self._connections = []
        for t in tokens:
            s = requests.Session()
            s.headers.update({ 'X-Api-Key': t })
            self._connections.append(s)
        self._index = random.randint(0, len(self._connections) - 1)
        self._num_queries = 0
        self._url = url

    def request(self, path, method='GET', payload='', retries=0, required=None):
        for _ in range(retries + 1):
            self._num_queries += 1
            time.sleep(1.1 / len(self._connections))
            conn = self._connections[self._index]
            self._index = (self._index + 1) % len(self._connections)
            r = conn.request(method, self._url + path, data=payload)
            response = r.json()
            if r.status_code == 429:  # too many requests, just retry
                print("Warning:", response["message"], file=sys.stderr)
                continue
            if "errorMessage" in response:
                print("Error on %s %s." % (path, payload), response["errorMessage"],
                      response.get("errorType"), response.get("stackTrace"), file=sys.stderr)
            elif r.status_code == 403:  # forbidden, probably no access to the segment in advanced mode
                print("Warning:", response["message"], path, conn.headers, file=sys.stderr)
            elif required and required not in response:
                print("Format error on %s %s." % (path, payload), file=sys.stderr)
                pprint.pp(response, sys.stderr)
            return response
        return {}

    def print_stats(self):
        print(len(self._connections), "connections", self._num_queries, "queries")


def parse_utc(date):
    if isinstance(date, datetime.datetime):
        return date.astimezone(datetime.timezone.utc)
    return datetime.datetime.fromisoformat(date.replace("Z", "+00:00")) if date and date != "NaT" else None


def parse_utc_dict(dict, key):
    return parse_utc(dict.get(key) or '1970-01-01T00:00:00+00:00')


def load_json_if_stale(json_file, clear=False, verbose=0):
    """Load GeoJSON features if file is stale (>30 min old) or missing.
    Returns features list, or None if the file is fresh (caller should skip regeneration)."""
    delta = datetime.timedelta(minutes=30)
    if not os.path.exists(json_file) or clear:
        return []
    with open(json_file, encoding="utf8") as f:
        old_json = json.load(f)
    last_mod = parse_utc_dict(old_json.get("properties", {}), "created_at")
    if datetime.datetime.now(datetime.timezone.utc) - last_mod < delta:
        if verbose:
            print(f"Not recreating {json_file}, it is less than {delta} old.")
        return None
    return old_json.get("features", [])


def save_json(json_file, content):
    with open(json_file + ".new", "w", encoding="utf8") as output:
        json.dump(content, output, indent=2)
    os.rename(json_file + ".new", json_file)


def year_file(parquet, year):
    base = parquet[:-len(".parquet")] if parquet.endswith(".parquet") else parquet
    return f"{base}_{year}.parquet"


def merge_parquet(new_df, parquet):
    """Upsert new_df into parquet, replacing existing rows with the same (segment_id, date)."""
    if not os.path.exists(parquet):
        return new_df
    existing = pd.read_parquet(parquet)
    new_keys = new_df.set_index(["segment_id", "date"]).index
    keep = ~existing.set_index(["segment_id", "date"]).index.isin(new_keys)
    return pd.concat([existing[keep], new_df], ignore_index=True)


def load_parquet_years(parquet, years=None, segments=None):
    """Load year-split parquet files. If years is None, load all available years.
    Falls back to single file for backward compatibility."""
    if years is None:
        base = parquet[:-len(".parquet")] if parquet.endswith(".parquet") else parquet
        year_files = sorted(glob.glob(f"{base}_*.parquet"))
    else:
        year_files = [yf for y in years if os.path.exists(yf := year_file(parquet, y))]
    if not year_files:
        if os.path.exists(parquet):
            df = pd.read_parquet(parquet)
            return df if years is None else df[df['date'].dt.year.isin(years)]
        return None
    parts = []
    for yf in year_files:
        df = pd.read_parquet(yf)
        if segments:
            parts.append(df[df['segment_id'].isin(segments)])
            del df
        else:
            parts.append(df)
    return pd.concat(parts, ignore_index=True)


def write_csv(filename, df_out):
    """Write df_out to a gzip-compressed CSV file. No-op if df_out is None."""
    if df_out is None:
        return
    with gzip.open(filename, "wt") as csv_file:
        df_out.to_csv(csv_file, index=False)


def load_segments(json_file):
    """Load segments from GeoJSON file. Returns dict keyed by segment_id."""
    if not os.path.exists(json_file):
        return {}
    with open(json_file, encoding="utf8") as f:
        return {feat["properties"]["segment_id"]: feat["properties"]
                for feat in json.load(f).get("features", [])}


def save_segments(segments, json_file):
    """Write updated backup timestamps back into the GeoJSON file."""
    with open(json_file, encoding="utf8") as segment_file:
        content = json.load(segment_file)
    for segment in content.get("features", []):
        sid = segment["properties"]["segment_id"]
        if sid in segments:
            segment["properties"] = segments[sid]
    save_json(json_file, content)


_HEADERS = {"User-Agent": "WeCount/1.0 (https://github.com/codeforberlin/we-count)"}


def _get_with_retry(url, params, retries=3, retry_wait=60, timeout=120, stream=False):
    """GET with retry on transient errors (5xx, connection/timeout).
    Returns the response on success, None on permanent failure."""
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=_HEADERS, timeout=timeout, stream=stream)
        except requests.exceptions.RequestException as e:
            print(f"Connection error fetching {url}: {e}, retrying in {retry_wait}s ({attempt + 1}/{retries})", file=sys.stderr)
            if attempt + 1 < retries:
                time.sleep(retry_wait)
            continue
        if r.status_code < 400:
            return r
        if r.status_code < 500:
            print(f"HTTP {r.status_code} fetching {url}: permanent error.", file=sys.stderr)
            return None
        print(f"HTTP {r.status_code} fetching {url}, retrying in {retry_wait}s ({attempt + 1}/{retries})", file=sys.stderr)
        if attempt + 1 < retries:
            time.sleep(retry_wait)
    print(f"Giving up on {url} after {retries} attempts.", file=sys.stderr)
    return None


def fetch_all(url, params=None, retries=3, retry_wait=60):
    """Paginated GET against an OGC SensorThings API — follows @iot.nextLink."""
    result = []
    while url:
        r = _get_with_retry(url, params, retries, retry_wait)
        if r is None:
            return []
        data = r.json()
        result.extend(data.get("value", []))
        url = data.get("@iot.nextLink")
        params = None  # params are encoded in nextLink
    return result


def fetch_arcgis_features(layer_url, params, page_size=1000, retries=3, retry_wait=60):
    """Paginated query against an ArcGIS Feature Server layer."""
    result = []
    offset = 0
    url = layer_url + "/query"
    while True:
        r = _get_with_retry(url, {**params, "resultOffset": offset, "resultRecordCount": page_size}, retries, retry_wait)
        if r is None:
            return []
        data = r.json()
        features = data.get("features", [])
        result.extend(features)
        if not data.get("exceededTransferLimit"):
            break
        offset += len(features)
    return result


def parse_options(options):
    if os.path.exists(options.secrets_file):
        with open(options.secrets_file, encoding="utf8") as sf:
            options.secrets = json.load(sf)
    if getattr(options, "url", None) and "://" not in options.url:
        options.url = "https://" + options.url
    return options


def get_options(args=None, json_default="sensor.json", url_default="telraam-api.net",
                parquet_default="data.parquet", year_default=None):
    parser = argparse.ArgumentParser()
    # Berlin as in https://github.com/DLR-TS/sumo-berlin
    parser.add_argument("-b", "--bbox", default="12.78509,52.17841,13.84308,52.82727",
                        help="bounding box to retrieve in geo coordinates west,south,east,north")
    parser.add_argument("-u", "--url", default=url_default,
                        help="Download from the given server")
    parser.add_argument("-s", "--secrets-file", default="secrets.json",
                        metavar="FILE", help="Read Telraam API credentials from FILE")
    parser.add_argument("-j", "--json-file", default=json_default,
                        metavar="FILE", help="Write / read Geo-JSON for segments to / from FILE")
    parser.add_argument("--excel", action="store_true", default=False,
                        help="create excel output file")
    parser.add_argument("--clear", action="store_true", default=False,
                        help="recreate data even if it is present")
    parser.add_argument("-p", "--parquet", metavar="FILE", default=parquet_default,
                        help="Data storage file")
    parser.add_argument("--csv",
                        help="Output prefix for monthly csv / excel files")
    parser.add_argument("--csv-segments",
                        help="Output prefix for csv / excel segment files")
    parser.add_argument("-y", "--year", "--csv-start-year", type=int, default=year_default,
                        help="First year to retrieve when writing csv")
    parser.add_argument("-r", "--retry", type=int, default=1,
                        help="number of retries on failure")
    parser.add_argument("--max-prop-updates", type=int, default=10,
                        help="maximum number of segment property updates per run")
    parser.add_argument("-a", "--advanced", action="store_true", default=False,
                        help="use the advanced API with quarterly data")
    parser.add_argument("--segments",
                        help="only process the given segment(s)")
    parser.add_argument("--limit", type=int,
                        help="process segments in batches of N, sorted by oldest backup first")
    parser.add_argument("--dump", metavar="FILE",
                        help="dump all JSON answers to the given file (for debugging)")
    parser.add_argument("--single-line-output", metavar="FILE",
                        help="write one JSON object per segment per line to FILE")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="increase verbosity")
    return parse_options(parser.parse_args(args=args))

def benchmark(func):
    """
    decorator for timing a function
    """
    def benchmark_wrapper(*args, **kwargs):
        started = time.time()
        now = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.localtime())
        print('function %s called at %s' % (func.__name__, now))
        sys.stdout.flush()
        result = func(*args, **kwargs)
        print('function %s finished after %f seconds' %
              (func.__name__, time.time() - started))
        sys.stdout.flush()
        return result
    return benchmark_wrapper


class Benchmarker:
    """
    class for benchmarking a function using a "with"-statement.
    Preferable over the "benchmark" function for the following use cases
    - benchmarking a code block that isn't wrapped in a function
    - benchmarking a function only in some calls
    """
    def __init__(self, active, description):
        self.active = active
        self.description = description

    def __enter__(self):
        self.started = time.time()

    def __exit__(self, *args):
        if self.active:
            duration = time.time() - self.started
            print("%s finished after %s" % (self.description, duration))


def add_month(offset: int, year: int, month: int):
    month += offset
    while month > 12:
        year += 1
        month -= 12
    while month < 1:
        year -= 1
        month += 12
    return year, month


def benchmark(func):
    """
    decorator for timing a function
    """
    def benchmark_wrapper(*args, **kwargs):
        started = time.time()
        now = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.localtime())
        print('function %s called at %s' % (func.__name__, now))
        sys.stdout.flush()
        result = func(*args, **kwargs)
        print('function %s finished after %f seconds' %
              (func.__name__, time.time() - started))
        sys.stdout.flush()
        return result
    return benchmark_wrapper


class Benchmarker:
    """
    class for benchmarking a function using a "with"-statement.
    Preferable over the "benchmark" function for the following use cases
    - benchmarking a code block that isn't wrapped in a function
    - benchmarking a function only in some calls
    """
    def __init__(self, active, description):
        self.active = active
        self.description = description

    def __enter__(self):
        self.started = time.time()

    def __exit__(self, *args):
        if self.active:
            duration = time.time() - self.started
            print("%s finished after %s" % (self.description, duration))
