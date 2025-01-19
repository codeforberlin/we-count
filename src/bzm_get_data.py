#!/usr/bin/env python3
# Copyright (c) 2024-2025 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    bzm_get_data.py
# @author  Egbert Klaassen
# @author  Michael Behrisch
# @date    2025-01-05

# This module contains a collection of functions to retrieve the data and return a pandas data frame.
# It can also be used as a script to just save the data to a file.

import os

from babel.dates import get_day_names, get_month_names
from datetime import datetime
import pandas as pd

from common import add_month

ASSET_DIR = os.path.join(os.path.dirname(__file__), 'assets')
CSV_DIR = os.path.join(os.path.dirname(__file__), '..', 'csv')
VERBOSE = False


# Save df files for development/debugging purposes
def save_df(df:pd.DataFrame, file_name: str) -> None:
    path = os.path.join(ASSET_DIR, file_name)
    if VERBOSE:
        print('Saving '+ path)
    if file_name.endswith(".xlsx"):
        df.to_excel(path, index=False)
    else:
        df.to_csv(path, index=False)


# Function to fill missing dates for each segment
def fill_missing_dates(df):
    result_df = pd.DataFrame()

    for segment in df['segment_id'].unique():
        # Remove duplicate date local within segment
        segment_df = df[df['segment_id'] == segment].drop_duplicates(subset=['date_local'], keep='last').set_index('date_local')

        #segment_df.set_index('date_local', inplace=True)
        full_date_range = pd.date_range(start=segment_df.index.min(), end=segment_df.index.max(), freq='h')
        segment_df = segment_df.reindex(full_date_range)

        # Ensure new date entries are populated
        segment_df['segment_id'] = segment
        result_df = pd.concat([result_df, segment_df])

    return result_df.reset_index().rename(columns={'index': 'date_local'})


def get_locations(filepath=os.path.join(os.path.dirname(__file__), 'assets', 'bzm_telraam_segments_2025.geojson')):
    df_geojson = pd.read_json(filepath)

    # Flatten the json structure
    df_geojson = pd.json_normalize(df_geojson['features'])

    # Remove 'properties' from column names for ease of use
    df_geojson.columns = df_geojson.columns.str.replace('properties.', '', regex=True)

    # Drop uptime and v85 to avoid duplicates as these will come from traffic data
    df_geojson = df_geojson.drop(['uptime', 'v85'], axis=1)

    # Replace "list" entries (Telraam!) with none
    for i in range(len(df_geojson)):
        if isinstance(df_geojson['osm.width'].values[i],list):
            df_geojson['osm.width'].values[i]=''
        if isinstance(df_geojson['osm.lanes'].values[i],list):
            df_geojson['osm.lanes'].values[i]=''
        if isinstance(df_geojson['osm.maxspeed'].values[i],list):
            df_geojson['osm.maxspeed'].values[i]=''
        if isinstance(df_geojson['osm.name'].values[i],list):
            df_geojson['osm.name'].values[i]=pd.NA

    # Remove segments w/o street name
    nan_rows = df_geojson[df_geojson['osm.name'].isnull()]
    return df_geojson.drop(nan_rows.index)


def get_traffic_data(months=4):
    month, year = datetime.now().month, datetime.now().year
    all_files = []
    for offset in range(months):
        file = "bzm_telraam_%s_%02i.csv.gz" % add_month(-offset, year, month)
        path = os.path.join(CSV_DIR, file)
        if not os.path.exists(path):
            if VERBOSE:
                print(f'No local copy, retrieving {file} from the web.')
            path = 'https://berlin-zaehlt.de/csv/' + file
        all_files.append(path)
    # Retrieve file links
    if VERBOSE:
        print('Getting traffic data files...')
    df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

    # Change date_local to datetime
    df['date_local'] = pd.to_datetime(df['date_local'])

    # Fill missing dates
    return fill_missing_dates(df)


def merge_data(locations, cached=True):
    if cached:
        return pd.read_csv(os.path.join(ASSET_DIR, 'traffic_df_2024_Q4_2025_YTD.csv.gz'))
    ### Merge traffic data with geojson information, select columns, define data formats and add date_time columns
    if VERBOSE:
        print('Combining traffic and geojson data...')
    df_comb = pd.merge(get_traffic_data(), locations, on='segment_id', how='outer')

    # Remove rows w/o names after merging with csv files containing segment_id w/o osm.name
    if VERBOSE:
        print('Removing rows w/o osm.name or date_local entries')
    nan_rows = df_comb[df_comb['osm.name'].isnull()]
    df_comb = df_comb.drop(nan_rows.index)
    nan_rows = df_comb[df_comb['date_local'].isnull()]
    df_comb = df_comb.drop(nan_rows.index)

    if VERBOSE:
        print('Creating df with selected columns')
    selected_columns = ['date_local','segment_id','uptime','ped_total','bike_total','car_total','heavy_total','v85','car_speed0','car_speed10','car_speed20','car_speed30','car_speed40','car_speed50','car_speed60','car_speed70','osm.name','osm.highway','osm.length','osm.width','osm.lanes','osm.maxspeed']
    traffic_df = pd.DataFrame(df_comb, columns=selected_columns)

    if VERBOSE:
        print('Break down date_local to new columns...')
    traffic_df.insert(0, 'year', traffic_df['date_local'].dt.year)
    traffic_df.insert(0, 'year_month', traffic_df['date_local'].dt.strftime('%Y/%m'))
    traffic_df.insert(0, 'month', traffic_df['date_local'].dt.month)
    traffic_df.insert(0, 'year_week', traffic_df['date_local'].dt.strftime('%Y/%U'))
    traffic_df.insert(0, 'weekday', traffic_df['date_local'].dt.dayofweek)
    traffic_df.insert(0, 'day', traffic_df['date_local'].dt.day)
    traffic_df.insert(0, 'hour', traffic_df['date_local'].dt.hour)
    traffic_df.insert(0, 'date', traffic_df['date_local'].dt.strftime('%Y/%m/%d'))

    if VERBOSE:
        print('Exchange time data for labels...')
    traffic_df['weekday'] = traffic_df['weekday'].map(get_day_names('abbreviated', locale="en"))
    traffic_df['month'] = traffic_df['month'].map(get_month_names('abbreviated', locale="en"))
    return traffic_df.reset_index()


if __name__ == "__main__":
    VERBOSE = True
    traffic_df = merge_data(get_locations(), False)
    # save_df(traffic_df, "traffic_df_2024_Q4_2025_YTD.xlsx")
    save_df(traffic_df, "traffic_df_2024_Q4_2025_YTD.csv.gz")
    if VERBOSE:
        print('Finished.')
