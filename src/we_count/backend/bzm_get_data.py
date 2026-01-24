#!/usr/bin/env python3
# Copyright (c) 2024-2025 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    bzm_get_data.py
# @author  Egbert Klaassen
# @author  Michael Behrisch
# @date    2025-03-28

# This module contains a collection of functions to retrieve the data and return a pandas data frame.
# It can also be used as a script to just save the data to a file.

import argparse
import locale
import os
from datetime import datetime

import pandas as pd
import requests
import shapely.geometry
#from sqlalchemy import create_engine

from common import add_month, parse_options
#from datamodel import TrafficCount

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data')
CSV_DIR = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'csv')
OSM_COLUMNS = ['osm.' + x for x in ['osmid', 'name', 'length', 'lanes', 'maxspeed', 'highway',
                                    'address.city', 'address.suburb', 'address.postcode']]


def has_min_size(url, min_size=500):
    try:
        response = requests.head(url, allow_redirects=True)
        if 'Content-Length' in response.headers:
            size_in_bytes = int(response.headers['Content-Length'])
            return size_in_bytes > min_size
    except requests.RequestException as e:
        return False
    return False


def save_df(df:pd.DataFrame, file_name: str, verbose=False) -> None:
    path = os.path.join(DATA_DIR, file_name)
    if verbose:
        print('Saving '+ path)
    if file_name.endswith(".xlsx"):
        df.to_excel(path, index=False)
    elif file_name.endswith(".parquet"):
        df.to_parquet(path, index=False, compression='zstd')
    else:
        df.to_csv(path, index=False)


def get_locations(filepath="https://berlin-zaehlt.de/csv/bzm_telraam_segments.geojson"):
    local_file = os.path.join(DATA_DIR, os.path.basename(filepath))
    if has_min_size(filepath):
        response = requests.get(filepath)
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)
        with open(local_file, 'wb') as f:
            f.write(response.content)
    df_geojson = pd.read_json(local_file)

    # Flatten the json structure
    normalized = pd.json_normalize(df_geojson['features'])

    # Remove 'properties' from column names for ease of use
    normalized.columns = normalized.columns.str.replace('properties.', '', regex=True)

    # Convert geometry to WKT
    geometry = df_geojson['features'].apply(lambda x: shapely.geometry.shape(x['geometry']).wkt).rename('geometry')

    def get_hardware(x):
        if x['properties']['cameras']:
            return x['properties']['cameras'][0]['hardware_version']
        return 0
    hardware = df_geojson['features'].apply(get_hardware).rename('hardware_version')
    columns = ['segment_id', 'last_data_package'] + OSM_COLUMNS
    df_geojson = pd.concat([normalized[columns], geometry, hardware], axis=1)
    df_geojson['id_street'] = df_geojson['osm.name'].astype(str) + ' (' + df_geojson['segment_id'].astype(str) + ')'
    for col in OSM_COLUMNS:
        df_geojson[col] = df_geojson[col].astype(str)

    # Add street column for facet graphs - check efficiency!
    df_geojson['street_selection'] = df_geojson.loc[:, 'osm.name']
    df_geojson.loc[df_geojson['street_selection'] != 'does not exist', 'street_selection'] = 'All Streets'

    # Remove segments w/o street name
    nan_rows = df_geojson[df_geojson['osm.name'].isnull()]
    return df_geojson.drop(nan_rows.index)


def _read_csv(start_year=None, start_month=None, end_year=None, end_month=None, verbose=False):
    year, month = start_year, start_month
    all_files = []
    while (year, month) != (end_year, end_month):
        file = "bzm_telraam_%s_%02i.csv.gz" % (year, month)
        path = os.path.join(CSV_DIR, file)
        if not os.path.exists(path):
            if verbose:
                print(f'No local copy, retrieving {file} from the web.')
            path = 'https://berlin-zaehlt.de/csv/' + file
        all_files.append(path)
        year, month = add_month(1, year, month)
    if verbose:
        print('Getting traffic data files...')
    df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

    # Change date_local to datetime
    df['date_local'] = pd.to_datetime(df['date_local'])
    return df

# def _read_sql(options):
#     start = datetime.now() - timedelta(days=30*options.months)
#     engine = create_engine(options.database, echo=options.verbose > 1, future=True)
#     columns = "segment_id, date_utc AS date_local, uptime_rel AS uptime, car_speed_histogram"
#     for mode in TrafficCount.modes():
#         columns += f", {mode}_lft + {mode}_rgt AS {mode}_total"
#     table_df = pd.read_sql_query(f"SELECT {columns} FROM traffic_count WHERE date_utc > '{start}'", con=engine, parse_dates=["date_local"])
#     if len(table_df) > 0:
#         def hist_parse(x):
#             return TrafficCount.parse_histogram(x.iloc[0], x.iloc[1] * x.iloc[2])
#         histogram_df = table_df[["car_speed_histogram", "car_total", "uptime"]].apply(hist_parse, axis=1, result_type='expand')
#         histogram_df.columns = ["car_speed%s" % s for s in range(0, 80, 10)]
#     else:
#         histogram_df = pd.DataFrame(columns=["car_speed%s" % s for s in range(0, 80, 10)])
#     return pd.concat((table_df, histogram_df), axis=1)


def add_date_columns(traffic_df, verbose):
    if verbose:
        print('Break down date_local to new columns...')
    locale.setlocale(locale.LC_ALL, 'de_DE.UTF-8')
    traffic_df['year'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%Y')
    traffic_df['Monat'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%b')
    traffic_df['jahr_monat'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%b %Y')
    traffic_df['year_week'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%V-%G')
    traffic_df['Wochentag'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%a')
    traffic_df['date'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%d-%m-%Y')
    traffic_df['date_hour'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%d-%m-%y - %H')
    traffic_df['day'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%d')
    traffic_df.insert(0, 'hour', traffic_df['date_local'].dt.hour)

    locale.setlocale(locale.LC_ALL, 'en_GB.UTF-8')
    traffic_df['month'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%b')
    traffic_df['weekday'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%a')
    traffic_df['year_month'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%b %Y')


def merge_data(locations, cache_file=os.path.join(DATA_DIR, 'traffic_df_2024_Q4_2025_YTD.csv.gz'), traffic_data=None, verbose=False):
    if cache_file and os.path.exists(cache_file):
        return pd.read_csv(cache_file)
    if traffic_data is None:
        traffic_data = _read_csv(verbose=verbose)
    ### Merge traffic data with geojson information, select columns, define data formats and add date_time columns
    if verbose:
        print('Combining traffic and geojson data...')
    df_comb = pd.merge(traffic_data, locations, on='segment_id', how='outer')

    # Remove rows w/o names after merging with csv files containing segment_id w/o osm.name
    if verbose:
        print('Removing rows w/o osm.name or date_local entries')
    nan_rows = df_comb[df_comb['osm.name'].isnull()]
    df_comb = df_comb.drop(nan_rows.index)
    nan_rows = df_comb[df_comb['date_local'].isnull()]
    df_comb = df_comb.drop(nan_rows.index)

    if verbose:
        print('Creating df with selected columns')
    selected_columns = ['date_local','segment_id','uptime', 'hardware_version', 'last_data_package',
                        'ped_total','bike_total','car_total','heavy_total','v85',
                        'id_street','street_selection',
                        'car_speed0','car_speed10','car_speed20','car_speed30','car_speed40','car_speed50','car_speed60','car_speed70']
    traffic_df = pd.DataFrame(df_comb, columns=selected_columns)
    add_date_columns(traffic_df, verbose)
    return traffic_df.reset_index(drop=True)


def get_options(args=None, json_default="sensor.json"):
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--secrets-file", default="secrets.json",
                        metavar="FILE", help="Read database credentials from FILE")
    parser.add_argument("-j", "--json-file", default=json_default,
                        metavar="FILE", help="Write / read Geo-JSON for segments to / from FILE")
    parser.add_argument("--csv", action="store_true", default=False,
                        help="use CSV input")
    parser.add_argument("-d", "--database",
                        help="Database input file or URL")
    parser.add_argument("-o", "--output", default="traffic_df_%s.parquet",
                        help="Traffic data output file (format is derived from file extension)")
    parser.add_argument("-l", "--location-output", default="df_geojson.parquet",
                        help="Location data file (format is derived from file extension)")
    parser.add_argument("-m", "--months", type=int, default=4,
                        help="number of months to look back")
    parser.add_argument("-a", "--aggregate", type=int, default=3,
                        help="number of months to aggregate")
    parser.add_argument("-f", "--force", action="store_true", default=False,
                        help="create output files even if they exist and are up to date")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="increase verbosity, twice enables verbose sqlalchemy output")
    raw_options = parser.parse_args(args=args)
    if not raw_options.database:
        raw_options.csv = True
    return parse_options(raw_options)


def main(args=None):
    options = get_options(args)
    end_year, end_month = add_month(1, datetime.now().year, datetime.now().month)
    year, month = add_month(-options.months, datetime.now().year, datetime.now().month)
    month = ((month - 1) // options.aggregate) * options.aggregate + 1
    locations = get_locations()
    save_df(locations, options.location_output, options.verbose)
    while (year, month) < (end_year, end_month):
        yearp, monthp = add_month(options.aggregate, year, month)
        out_file = options.output % ("%s_%02i-%s_%02i" % ((year, month) + add_month(-1, yearp, monthp)))
        if add_month(options.aggregate, yearp, monthp) < (end_year, end_month) and os.path.exists(os.path.join(DATA_DIR, out_file)) and not options.force:
            year, month = yearp, monthp
            continue
        if (yearp, monthp) > (end_year, end_month):
            yearp, monthp = end_year, end_month
        traffic_df = _read_csv(year, month, yearp, monthp, options.verbose) if options.csv else _read_sql(options)
        merged_df = merge_data(locations, None, traffic_df)
        save_df(merged_df, out_file, options.verbose)
        if options.verbose:
            print('Finished.')
        del merged_df
        year, month = yearp, monthp


if __name__ == "__main__":
    main()
