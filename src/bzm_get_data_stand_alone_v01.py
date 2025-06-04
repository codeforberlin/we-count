#!/usr/bin/python3.13
# Copyright (c) 2024-2025 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    bzm_get_data.py
# @author  Egbert Klaassen
# @author  Michael Behrisch
# @date    2025-06-04

import os
import pandas as pd
import requests
import geopandas as gpd
from bs4 import BeautifulSoup
import json
import locale
#from memory_profiler import profile

# For debugging purposes
def output_excel(df, file_name):
    path = os.path.join(ASSET_DIR, file_name + '.xlsx')
    df.to_excel(path, index=False)
def output_csv(df, file_name):
    path = os.path.join(ASSET_DIR, file_name + '.csv')
    df.to_csv(path, index=False)

def get_file_size(url):
    try:
        response = requests.head(url, allow_redirects=True)
        if 'Content-Length' in response.headers:
            size_in_bytes = int(response.headers['Content-Length'])
            return size_in_bytes
        else:
            return "File size not available in headers."
    except requests.RequestException as e:
        return f"An error occurred: {e}"

verbose = False

ASSET_DIR = os.path.join(os.path.dirname(__file__), 'assets')

#@profile
#def my_function():

### Prepare geojson file with street information

# Read file, pre-select columns, check for file health, use off-line if size test failed
geojson_url = 'https://berlin-zaehlt.de/csv/bzm_telraam_segments.geojson'
geo_cols = ['segment_id', 'osm', 'cameras', 'geometry']
geojson_file_size = get_file_size(geojson_url)
if geojson_file_size > 500:
    geo_df = gpd.read_file(geojson_url, columns=geo_cols)
else:
    print('Suspected error, geojson_file_size: ' + str(geojson_file_size))
    geojson_path = os.path.join(ASSET_DIR, 'bzm_telraam_segments.geojson')
    geo_df = gpd.read_file(geojson_path, columns=geo_cols)

# Parse osm column
geo_df['parsed_osm'] = geo_df['osm'].apply(json.loads)
geo_df_osm = pd.json_normalize(geo_df['parsed_osm'])
geo_df_osm = geo_df_osm.drop(['lanes', 'width', 'last_osm_fetch', 'ref', 'junction', 'service', 'oneway', 'reversed'], axis=1)
#TODO: rename not required if aligned with bzm_v01.py
geo_df_osm = geo_df_osm.rename(columns={'name': 'osm.name', 'highway': 'osm.highway', 'address.city': 'osm.address.city',
             'address.suburb': 'osm.address.suburb', 'address.postcode': 'osm.address.postcode'})
# Drop remaining address columns
address_cols = geo_df_osm.columns[geo_df_osm.columns.str.startswith('address')]
geo_df_osm.drop(address_cols, axis=1, inplace=True)

# Remove rows with insufficient information
nan_rows = geo_df_osm[geo_df_osm['osm.name'].isnull()]
geo_df_osm.drop(nan_rows.index)

# Recombine and remove original osm columns
df_geojson = pd.concat([geo_df, geo_df_osm], axis=1)
df_geojson = df_geojson.drop(['osm', 'parsed_osm'], axis=1)
# Add id_street column for dropdown selection
df_geojson['id_street'] = df_geojson['osm.name'].astype(str) + ' (' + df_geojson['segment_id'].astype(str) + ')'

# Parse cameras column
geo_df['parsed_cameras'] = geo_df['cameras'].apply(json.loads)
geo_df_cameras = pd.json_normalize(geo_df['parsed_cameras'])
geo_df_cameras = geo_df_cameras.drop([1, 2, 3, 4, 5, 6, 7, 8, 9], axis=1)
geo_df_0 = pd.json_normalize(geo_df_cameras[0])
geo_df_0 = geo_df_0['hardware_version']

# Recombine and remove original osm columns
df_geojson = pd.concat([df_geojson, geo_df_0], axis=1)
df_geojson = df_geojson.drop(['cameras'], axis=1)

del geo_df_0, geo_df_cameras

# Replace "list" entries with none
for i in range(len(df_geojson)):
    if isinstance(df_geojson['maxspeed'].values[i],list):
        df_geojson['maxspeed'].values[i]=''

save_file_path = os.path.join(ASSET_DIR, 'df_geojson.csv.gz')
df_geojson.to_csv(save_file_path, index=False, compression='gzip')

### Get csv traffic file

# Retrieve file links
if verbose:
    print('Getting traffic data files...')
url = 'https://berlin-zaehlt.de/csv/'
page = requests.get(url).text
soup = BeautifulSoup(page, 'html.parser')
filename_startswith = 'bzm_telraam_'
links = [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').startswith(filename_startswith)]

# Add file contents to Dataframe
df_csv_append = pd.DataFrame()

for link in links:

    # Get filename from link
    filename = link.split('/')[-1]

    # Loop through gz files, filter by "start with" string, add to Dataframe
    if filename[12:16] in ['2023', '2024', '2025']:
        if verbose:
            print('Processing: ' + filename)
        use_cols = ['segment_id', 'date_local', 'uptime', 'ped_total', 'bike_total', 'car_total', 'heavy_total', 'v85', 'car_speed0', 'car_speed10', 'car_speed20', 'car_speed30', 'car_speed40', 'car_speed50', 'car_speed60', 'car_speed70']
        df = pd.read_csv(os.path.join(url, filename), usecols=use_cols, parse_dates=['date_local'],compression='gzip', header=0, sep=',', quotechar='"')
        df_csv_append = df_csv_append._append(df, ignore_index=True)

    # Alternative: Loop through gz files, filter by "contains substrings", add to Dataframe
    # substrings= ['_07','_08','_09']
    #substrings = ['2024_10','2024_11','2024_12', '2025_01']
    #if any(sub in filename for sub in substrings):
    #    if verbose:
    #    print('Processing: ' + filename)
    #    df = pd.read_csv(os.path.join(url,filename), compression='gzip', header=0, sep=',', quotechar='"')
    #    df_csv_append = df_csv_append._append(df, ignore_index=True)


### Merge traffic data with geojson information, select columns, add date_time columns and define data formats
if verbose:
    print('Combining traffic and geojson data...')

traffic_df = pd.merge(df_csv_append, df_geojson, on = 'segment_id', how = 'left')

# Remove dataframe from memory
del df_geojson, df_csv_append

if verbose:
    print('Creating df with selected columns')
#TODO: remove "osm", needs bzm_v01 to be updated
traffic_df = traffic_df.rename(columns={'name': 'osm.name', 'highway': 'osm.highway'})

if verbose:
    print('Drop rows with critical data lacking...')
nan_rows = traffic_df[traffic_df['date_local'].isnull()]
traffic_df = traffic_df.drop(nan_rows.index)
nan_rows = traffic_df[traffic_df['osm.name'].isnull()]
traffic_df = traffic_df.drop(nan_rows.index)
traffic_cols = ['ped_total', 'bike_total', 'car_total', 'heavy_total']
traffic_df['check_sum'] = traffic_df[traffic_cols].sum(axis=1)
nan_rows = traffic_df[traffic_df['check_sum'] == 0]
traffic_df = traffic_df.drop(nan_rows.index)
traffic_df = traffic_df.drop(['check_sum'], axis=1)

# Add street column for facet graphs - check efficiency!
traffic_df['street_selection'] = traffic_df.loc[:, 'osm.name']
traffic_df.loc[traffic_df['street_selection'] != 'does not exist', 'street_selection'] = 'All Streets'
traffic_df = traffic_df.drop(['osm.name'], axis=1)

if verbose:
    print('Break down date_local to formatted string columns, except "hour"...')

# Initiation with German
locale.setlocale(locale.LC_ALL, 'de_DE.UTF-8')

traffic_df['year'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%Y')
traffic_df['Monat'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%b')
traffic_df['jahr_monat'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%b %Y')
traffic_df['year_week'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%U/%Y')
traffic_df['Wochentag'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%a')
traffic_df['date'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%d/%m/%Y')
traffic_df['date_hour'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%d/%m/%y - %H')
traffic_df['day'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%d')
traffic_df.insert(0, 'hour', traffic_df['date_local'].dt.hour) # In case of csv.gz download!

locale.setlocale(locale.LC_ALL, 'en_GB.UTF-8')
traffic_df['month'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%b')
traffic_df['weekday'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%a')
traffic_df['year_month'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%b %Y')

# Save data package to file - change file name!
if verbose:
    print("Saving data package...")
save_file_path = os.path.join(ASSET_DIR, 'traffic_df_2023_2024_2025_YTD.csv.gz')
traffic_df.to_csv(save_file_path, index=False, compression='gzip')

#    return

#my_function()

if verbose:
    print('Finished.')