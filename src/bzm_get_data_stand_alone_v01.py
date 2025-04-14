#!/usr/bin/python3.13
# Copyright (c) 2024-2025 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    bzm_get_data.py
# @author  Egbert Klaassen
# @author  Michael Behrisch
# @date    2025-04-14

import os
import pandas as pd
import requests
import geopandas as gpd
from bs4 import BeautifulSoup
from pathlib import Path
import json

# For debugging purposes
def output_excel(df, file_name, path):
    path = os.path.join(path, file_name + '.xlsx')
    df.to_excel(path, index=False)
def output_csv(df, file_name, path):
    path = os.path.join(path, file_name + '.csv')
    df.to_csv(path, index=False)


THIS_FOLDER = Path(__file__).parent.resolve()
assets_file_path = THIS_FOLDER / 'assets/'

### Get geojson file with street information

# Geopandas df route
geojson_url = 'https://berlin-zaehlt.de/csv/bzm_telraam_segments.geojson'
geo_df = gpd.read_file(geojson_url)

geo_df['parsed_osm'] = geo_df['osm'].apply(json.loads)
geo_df_osm = pd.json_normalize(geo_df['parsed_osm'])
df_geojson = pd.concat([geo_df, geo_df_osm], axis=1)

# Drop uptime and v85 to avoid duplicates (these will come from traffic data)
df_geojson = df_geojson.drop(['uptime', 'v85'], axis=1)
# Drop unnecessary load
df_geojson = df_geojson.drop(['timezone', 'parsed_osm'], axis=1)

# Replace "list" entries (Telraam!) with none
for i in range(len(df_geojson)):
    if isinstance(df_geojson['width'].values[i],list):
        df_geojson['width'].values[i]=''
    if isinstance(df_geojson['lanes'].values[i],list):
        df_geojson['lanes'].values[i]=''
    if isinstance(df_geojson['maxspeed'].values[i],list):
        df_geojson['maxspeed'].values[i]=''


### Get csv traffic files

# Retrieve file links
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
        print('Processing: ' + filename)
        df = pd.read_csv(os.path.join(url, filename), compression='gzip', header=0, sep=',', quotechar='"')
        df_csv_append = df_csv_append._append(df, ignore_index=True)

    # Alternative: Loop through gz files, filter by "contains substrings", add to Dataframe
    # substrings= ['_07','_08','_09']
    #substrings = ['2024_10','2024_11','2024_12', '2025_01']
    #if any(sub in filename for sub in substrings):
    #    print('Processing: ' + filename)
    #    df = pd.read_csv(os.path.join(url,filename), compression='gzip', header=0, sep=',', quotechar='"')
    #    df_csv_append = df_csv_append._append(df, ignore_index=True)


### Merge traffic data with geojson information, select columns, add date_time columns and define data formats
print('Combining traffic and geojson data...')
df_comb = pd.merge(df_csv_append, df_geojson, on = 'segment_id', how = 'left')

print('Creating df with selected columns')
#TODO: remove "osm", needs bzm_v01 to be updated
df_comb = df_comb.rename(
    columns={'name': 'osm.name', 'highway': 'osm.highway', 'address.city': 'osm.address.city',
             'address.suburb': 'osm.address.suburb', 'address.postcode': 'osm.address.postcode'})
selected_columns = ['date_local','segment_id','uptime','ped_lft','ped_rgt','ped_total','bike_lft','bike_rgt','bike_total','car_lft','car_rgt','car_total','heavy_lft','heavy_rgt','heavy_total','v85','car_speed0','car_speed10','car_speed20','car_speed30','car_speed40','car_speed50','car_speed60','car_speed70','osm.name','highway','length','width','lanes','maxspeed']
traffic_df = pd.DataFrame(df_comb, columns=selected_columns)
traffic_df['date_local'] = pd.to_datetime(traffic_df['date_local'])

print('Drop empty rows...')
nan_rows = traffic_df[traffic_df['date_local'].isnull()]
traffic_df = traffic_df.drop(nan_rows.index)
nan_rows = traffic_df[traffic_df['osm.name'].isnull()]
traffic_df = traffic_df.drop(nan_rows.index)

print('Break down date_local to formatted string columns, except "hour"...')
traffic_df['year'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%Y')
traffic_df['month'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%b')
traffic_df['year_month'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%b %Y')
traffic_df['year_week'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%U/%Y')
traffic_df['weekday'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%a')
traffic_df['date'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%d/%m/%Y')
traffic_df['date_hour'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%d/%m/%y - %H')
traffic_df['day'] = pd.to_datetime(traffic_df.date_local).dt.strftime('%d')
traffic_df.insert(0, 'hour', traffic_df['date_local'].dt.hour) # In case of csv.gz download!

# Save data package to file - change file name!
print("Saving data package...")
THIS_FOLDER = Path(__file__).parent.resolve()
traffic_file_path = THIS_FOLDER / 'assets/traffic_df_2023_2024_2025_YTD.csv.gz'
traffic_df.to_csv(traffic_file_path, index=False, compression='gzip')

print('Finished.')